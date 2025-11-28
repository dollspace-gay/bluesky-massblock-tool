import json
import os
import sys
import time
import warnings
from datetime import datetime, timedelta
from getpass import getpass
from pathlib import Path
from urllib.parse import urlparse

warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

from atproto import Client, models
from atproto.exceptions import AtProtocolError


# Rate limit constants based on Bluesky API limits
# See: https://docs.bsky.app/docs/advanced-guides/rate-limits
CREATES_PER_HOUR = 1500  # Conservative limit (actual is ~1666)
DELAY_BETWEEN_CREATES = 3600 / CREATES_PER_HOUR  # ~2.4 seconds per create
DELAY_BETWEEN_REQUESTS = 0.1  # 100ms for read operations
MAX_RETRIES = 5
INITIAL_BACKOFF = 30.0  # Initial backoff in seconds for rate limits
STATE_FILE_NAME = "block_followers_state.json"


class BatchState:
    """Persistent state for resumable batch operations."""

    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.data = {
            "target_did": None,
            "target_handle": None,
            "list_uri": None,
            "followers": [],  # List of DIDs to process
            "processed": [],  # List of DIDs already processed
            "added": 0,
            "skipped": 0,
            "failed": 0,
            "started_at": None,
            "last_updated": None,
            "hourly_count": 0,
            "hour_started": None,
        }

    def load(self) -> bool:
        """Load state from file. Returns True if state exists."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    self.data = json.load(f)
                return True
            except (json.JSONDecodeError, IOError) as e:
                print(f"Warning: Could not load state file: {e}")
                return False
        return False

    def save(self) -> None:
        """Save current state to file."""
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.state_file, "w") as f:
            json.dump(self.data, f, indent=2)

    def reset_hourly_if_needed(self) -> None:
        """Reset hourly counter if an hour has passed."""
        if self.data["hour_started"]:
            hour_started = datetime.fromisoformat(self.data["hour_started"])
            if datetime.now() - hour_started >= timedelta(hours=1):
                self.data["hourly_count"] = 0
                self.data["hour_started"] = datetime.now().isoformat()
        else:
            self.data["hour_started"] = datetime.now().isoformat()

    def can_proceed(self) -> bool:
        """Check if we can make another create request this hour."""
        self.reset_hourly_if_needed()
        return self.data["hourly_count"] < CREATES_PER_HOUR

    def wait_for_next_hour(self) -> None:
        """Wait until the next hour window."""
        if not self.data["hour_started"]:
            return

        hour_started = datetime.fromisoformat(self.data["hour_started"])
        next_hour = hour_started + timedelta(hours=1)
        wait_seconds = (next_hour - datetime.now()).total_seconds()

        if wait_seconds > 0:
            print(f"\n--- Hourly limit reached ({CREATES_PER_HOUR} creates) ---")
            print(f"Waiting {wait_seconds / 60:.1f} minutes until next window...")
            print(f"Resume time: {next_hour.strftime('%H:%M:%S')}")
            print("(Press Ctrl+C to stop - progress is saved)\n")

            # Wait in chunks so we can show progress and respond to interrupts
            while wait_seconds > 0:
                chunk = min(60, wait_seconds)
                time.sleep(chunk)
                wait_seconds -= chunk
                if wait_seconds > 0:
                    print(f"  {wait_seconds / 60:.1f} minutes remaining...")

            # Reset for new hour
            self.data["hourly_count"] = 0
            self.data["hour_started"] = datetime.now().isoformat()
            self.save()

    def increment_hourly(self) -> None:
        """Increment the hourly create counter."""
        self.data["hourly_count"] += 1

    def get_remaining(self) -> list:
        """Get list of DIDs not yet processed."""
        processed_set = set(self.data["processed"])
        return [did for did in self.data["followers"] if did not in processed_set]

    def mark_processed(self, did: str, status: str) -> None:
        """Mark a DID as processed with given status."""
        self.data["processed"].append(did)
        if status == "added":
            self.data["added"] += 1
        elif status == "skipped":
            self.data["skipped"] += 1
        else:
            self.data["failed"] += 1

    def delete(self) -> None:
        """Delete the state file."""
        if self.state_file.exists():
            self.state_file.unlink()


def resolve_user_did(client: Client, user_input: str) -> tuple[str, str]:
    """
    Resolves a user identifier (handle, DID, or profile URL) to a DID.

    Args:
        client: An authenticated atproto Client.
        user_input: A Bluesky handle, DID, or profile URL.

    Returns:
        A tuple containing the DID (str) and the handle (str).
    """
    if user_input.startswith("http://") or user_input.startswith("https://"):
        parsed_url = urlparse(user_input)
        path_parts = parsed_url.path.strip("/").split("/")

        if len(path_parts) < 2 or path_parts[0] != "profile":
            raise ValueError(f"Invalid Bluesky profile URL format: {user_input}")

        handle_or_did = path_parts[1]
    else:
        handle_or_did = user_input

    if handle_or_did.startswith("did:"):
        try:
            profile = client.get_profile(actor=handle_or_did)
            return profile.did, profile.handle
        except AtProtocolError as e:
            raise ValueError(f"Could not resolve DID '{handle_or_did}': {e}")
    else:
        try:
            response = client.resolve_handle(handle=handle_or_did)
            return response.did, handle_or_did
        except AtProtocolError as e:
            raise ValueError(f"Could not resolve handle '{handle_or_did}': {e}")


def parse_list_url_to_uri(client: Client, url: str) -> tuple[str, str]:
    """
    Parses a Bluesky list URL and converts it into an AT URI.
    """
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.strip("/").split("/")

    if len(path_parts) != 4 or path_parts[0] != "profile" or path_parts[2] != "lists":
        raise ValueError(f"Invalid Bluesky List URL format: {url}")

    handle_or_did = path_parts[1]
    rkey = path_parts[3]

    if handle_or_did.startswith("did:"):
        did = handle_or_did
    else:
        try:
            response = client.resolve_handle(handle=handle_or_did)
            did = response.did
        except AtProtocolError as e:
            raise ValueError(f"Could not resolve handle '{handle_or_did}': {e}")

    at_uri = f"at://{did}/app.bsky.graph.list/{rkey}"
    return at_uri, did


def is_rate_limit_error(error: AtProtocolError) -> bool:
    """Check if an error is a rate limit error (HTTP 429)."""
    error_str = str(error).lower()
    return (
        "429" in error_str
        or "rate" in error_str
        or "too many" in error_str
        or "ratelimit" in error_str
    )


def fetch_all_followers(client: Client, target_did: str) -> list[dict]:
    """
    Fetches all followers of a given user.

    Returns:
        A list of dicts with 'did' and 'handle' keys.
    """
    all_followers = []
    cursor = None
    retries = 0

    print("  Fetching followers (this may take a while for large accounts)...")

    while True:
        try:
            time.sleep(DELAY_BETWEEN_REQUESTS)
            response = client.get_followers(actor=target_did, cursor=cursor, limit=100)

            if response.followers:
                for f in response.followers:
                    all_followers.append({"did": f.did, "handle": f.handle})
                print(f"    Fetched {len(all_followers)} followers...")

            cursor = response.cursor
            retries = 0  # Reset on success

            if not cursor:
                break

        except AtProtocolError as e:
            if is_rate_limit_error(e):
                retries += 1
                if retries >= MAX_RETRIES:
                    print(f"  Max retries reached. Got {len(all_followers)} followers.")
                    break
                wait = INITIAL_BACKOFF * (2 ** (retries - 1))
                print(f"  Rate limited fetching followers. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  Error fetching followers: {e}")
                break

    return all_followers


def add_to_blocklist(
    client: Client,
    did: str,
    handle: str,
    list_uri: str,
    list_owner_did: str,
) -> str:
    """
    Add a single user to blocklist.

    Returns:
        Status string: 'added', 'skipped', or 'failed'
    """
    retries = 0

    while retries < MAX_RETRIES:
        try:
            list_item_record = {
                "$type": "app.bsky.graph.listitem",
                "subject": did,
                "list": list_uri,
                "createdAt": client.get_current_time_iso(),
            }

            client.com.atproto.repo.create_record(
                models.ComAtprotoRepoCreateRecord.Data(
                    repo=list_owner_did,
                    collection="app.bsky.graph.listitem",
                    record=list_item_record,
                )
            )
            return "added"

        except AtProtocolError as e:
            error_str = str(e.args[0]) if e.args else str(e)

            if "duplicate record" in error_str.lower() or "already exists" in error_str.lower():
                return "skipped"

            if is_rate_limit_error(e):
                retries += 1
                if retries >= MAX_RETRIES:
                    return "failed"
                wait = INITIAL_BACKOFF * (2 ** (retries - 1))
                print(f"    Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"    Error: {e}")
                return "failed"

    return "failed"


def run_batch_process(
    client: Client,
    state: BatchState,
    list_uri: str,
    list_owner_did: str,
    follower_lookup: dict[str, str],
) -> None:
    """Run the batch blocking process with state persistence."""
    remaining = state.get_remaining()
    total = len(state.data["followers"])
    processed_count = len(state.data["processed"])

    print(f"\nProcessing {len(remaining)} remaining followers...")
    print(f"(Already processed: {processed_count}/{total})")
    print(f"Rate: ~{CREATES_PER_HOUR}/hour, ~{DELAY_BETWEEN_CREATES:.1f}s between adds")
    print("Press Ctrl+C to pause (progress is saved)\n")

    try:
        for did in remaining:
            # Check hourly limit
            if not state.can_proceed():
                state.save()
                state.wait_for_next_hour()

            handle = follower_lookup.get(did, "unknown")

            # Skip self
            if did == client.me.did:
                state.mark_processed(did, "skipped")
                print(f"  [~] Skipped @{handle} (that's you!)")
                state.save()
                continue

            # Add delay between creates
            time.sleep(DELAY_BETWEEN_CREATES)

            status = add_to_blocklist(client, did, handle, list_uri, list_owner_did)
            state.mark_processed(did, status)
            state.increment_hourly()

            current = len(state.data["processed"])
            if status == "added":
                print(f"  [{current}/{total}] [+] Added @{handle}")
            elif status == "skipped":
                print(f"  [{current}/{total}] [~] Skipped @{handle} (already in list)")
            else:
                print(f"  [{current}/{total}] [!] Failed @{handle}")

            # Save state periodically (every 10 users)
            if current % 10 == 0:
                state.save()

            # Progress update every 100 users
            if current % 100 == 0:
                remaining_count = total - current
                eta_hours = (remaining_count * DELAY_BETWEEN_CREATES) / 3600
                print(f"\n  --- Progress: {current}/{total}, ~{eta_hours:.1f} hours remaining ---\n")

    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving progress...")
        state.save()
        print(f"Progress saved. Run the script again to resume.")
        print(f"  Processed: {len(state.data['processed'])}/{total}")
        print(f"  Added: {state.data['added']}")
        print(f"  Skipped: {state.data['skipped']}")
        print(f"  Failed: {state.data['failed']}")
        sys.exit(0)

    # Final save
    state.save()


def add_followers_to_blocklist(
    target_user: str, list_url: str, username: str, password: str
) -> None:
    """
    Finds all followers of a target user and adds them to a blocklist.
    Supports resumable batch processing for large follower counts.
    """
    state_file = Path(STATE_FILE_NAME)
    state = BatchState(state_file)

    # Check for existing state
    if state.load():
        print(f"\nFound existing batch in progress!")
        print(f"  Target: @{state.data['target_handle']}")
        print(f"  Progress: {len(state.data['processed'])}/{len(state.data['followers'])}")
        print(f"  Added: {state.data['added']}, Skipped: {state.data['skipped']}, Failed: {state.data['failed']}")

        choice = input("\nResume this batch? (y/n/restart): ").lower().strip()
        if choice == "restart":
            state.delete()
            state = BatchState(state_file)
        elif choice != "y":
            print("Exiting.")
            sys.exit(0)

    # Login
    try:
        client = Client()
        client.login(username, password)
        print("Successfully logged in.")
    except AtProtocolError as e:
        print(f"Login failed: {e}")
        sys.exit(1)

    # If no existing state, set up new batch
    if not state.data["target_did"]:
        try:
            print("\nResolving target user...")
            target_did, target_handle = resolve_user_did(client, target_user)
            print(f"  Target: @{target_handle} ({target_did})")

            print("\nParsing blocklist URL...")
            list_uri, list_owner_did = parse_list_url_to_uri(client, list_url)
            print(f"  List URI: {list_uri}")

            if client.me.did != list_owner_did:
                print("\nWarning: You are not the list owner.")
                if input("Continue anyway? (y/n): ").lower() != "y":
                    sys.exit(0)

            print(f"\nFetching all followers of @{target_handle}...")
            followers = fetch_all_followers(client, target_did)
            print(f"  Found {len(followers)} followers.")

            if not followers:
                print("\nNo followers found. Nothing to do.")
                return

            # Estimate time
            hours_estimate = (len(followers) * DELAY_BETWEEN_CREATES) / 3600
            print(f"\n  Estimated time: ~{hours_estimate:.1f} hours")
            print(f"  (Rate limited to {CREATES_PER_HOUR}/hour)")

            if input("\nProceed with blocking? (y/n): ").lower() != "y":
                sys.exit(0)

            # Initialize state
            state.data["target_did"] = target_did
            state.data["target_handle"] = target_handle
            state.data["list_uri"] = list_uri
            state.data["followers"] = [f["did"] for f in followers]
            state.data["started_at"] = datetime.now().isoformat()
            state.data["hour_started"] = datetime.now().isoformat()
            state.save()

            # Build lookup for handles
            follower_lookup = {f["did"]: f["handle"] for f in followers}

        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
    else:
        # Resuming - need to get list info and rebuild handle lookup
        list_uri = state.data["list_uri"]
        list_owner_did = list_uri.split("/")[2]  # Extract from at://did/...

        # For resumed batches, we don't have handles cached, use DIDs
        follower_lookup = {did: did[:20] + "..." for did in state.data["followers"]}

    # Run the batch process
    run_batch_process(client, state, list_uri, list_owner_did, follower_lookup)

    # Complete
    print("\n--- Batch Complete ---")
    print(f"Successfully added: {state.data['added']}")
    print(f"Skipped (already exists or self): {state.data['skipped']}")
    print(f"Failed: {state.data['failed']}")

    # Clean up state file
    if input("\nDelete state file? (y/n): ").lower() == "y":
        state.delete()
        print("State file deleted.")

    print("Done!")


if __name__ == "__main__":
    print("--- Bluesky Follower Blocklist Tool ---")
    print("Block all followers of a specified user.")
    print("Supports resumable batch processing for large accounts.\n")

    target_user = os.environ.get("TARGET_USER") or input(
        "Enter the target user (handle, DID, or profile URL): "
    )
    list_url = os.environ.get("LIST_URL") or input("Enter the URL of your blocklist: ")
    bsky_username = os.environ.get("BSKY_USERNAME") or input(
        "Enter your Bluesky username (e.g., yourname.bsky.social): "
    )
    bsky_password = os.environ.get("BSKY_APP_PASSWORD") or getpass(
        "Enter your Bluesky App Password: "
    )

    if not all([target_user, list_url, bsky_username, bsky_password]):
        print("\nAll fields are required. Exiting.")
        sys.exit(1)

    print("\nStarting process...")
    add_followers_to_blocklist(target_user, list_url, bsky_username, bsky_password)
