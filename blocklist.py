import os
import sys
from getpass import getpass
from urllib.parse import urlparse
from atproto import Client, models
from atproto.exceptions import AtProtocolError

def parse_url_to_uri(client: Client, url: str) -> tuple[str, str]:
    """
    Parses a Bluesky web URL (for a post or list) and converts it into an AT URI.
    Also resolves the handle to a DID.

    Args:
        client: An authenticated atproto Client.
        url: The Bluesky URL to parse.

    Returns:
        A tuple containing the AT URI (str) and the owner's DID (str).
    """
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.strip('/').split('/')

    if len(path_parts) < 3 or path_parts[0] != 'profile':
        raise ValueError(f"Invalid Bluesky URL format: {url}")

    handle_or_did = path_parts[1]
    record_type_short = path_parts[2]
    
    if record_type_short == 'lists':
        if len(path_parts) != 4:
            raise ValueError(f"Invalid Bluesky List URL format: {url}")
        rkey = path_parts[3]
    elif record_type_short == 'post':
        if len(path_parts) != 4:
            raise ValueError(f"Invalid Bluesky Post URL format: {url}")
        rkey = path_parts[3]
    else:
        raise ValueError(f"Unsupported record type '{record_type_short}' in URL.")

    if handle_or_did.startswith('did:'):
        did = handle_or_did
    else:
        try:
            response = client.resolve_handle(handle=handle_or_did)
            did = response.did
        except AtProtocolError as e:
            raise ValueError(f"Could not resolve handle '{handle_or_did}': {e}")

    record_map = {
        'post': 'app.bsky.feed.post',
        'lists': 'app.bsky.graph.list'
    }
    nsid = record_map[record_type_short]

    at_uri = f"at://{did}/{nsid}/{rkey}"
    return at_uri, did

def add_interactors_to_blocklist(post_url: str, list_url: str, username: str, password: str):
    """
    Finds users who liked or reposted a post and adds them to a blocklist.

    Args:
        post_url: The URL of the target Bluesky post.
        list_url: The URL of the blocklist to add users to.
        username: Your Bluesky username (handle).
        password: Your Bluesky app password.
    """
    try:
        client = Client()
        client.login(username, password)
        print("✅ Successfully logged in.")
    except AtProtocolError as e:
        print(f"❌ Login failed: {e}")
        sys.exit(1)

    try:
        print("\nParsing URLs and resolving identifiers...")
        post_uri, _ = parse_url_to_uri(client, post_url)
        list_uri, list_owner_did = parse_url_to_uri(client, list_url)
        print(f"  - Target Post URI: {post_uri}")
        print(f"  - Target List URI: {list_uri}")

        if client.me.did != list_owner_did:
            print("\n⚠️ Warning: You are logged in as a different user than the list owner.")
            print("You may not have permission to add users to this list.")
            if input("Continue anyway? (y/n): ").lower() != 'y':
                sys.exit(0)

    except ValueError as e:
        print(f"❌ Error parsing URLs: {e}")
        sys.exit(1)

    print("\nFetching users who liked the post...")
    all_liker_profiles = []
    cursor = None
    while True:
        try:
            response = client.get_likes(uri=post_uri, cursor=cursor)
            if response.likes:
                liker_profiles = [like.actor for like in response.likes]
                all_liker_profiles.extend(liker_profiles)
            cursor = response.cursor
            if not cursor:
                break
        except AtProtocolError as e:
            print(f"❌ Could not fetch likes: {e}")
            break
    print(f"  - Found {len(all_liker_profiles)} likers.")

    print("\nFetching users who reposted the post...")
    all_reposters = []
    cursor = None
    while True:
        try:
            response = client.get_reposted_by(uri=post_uri, cursor=cursor)
            if response.reposted_by:
                all_reposters.extend(response.reposted_by)
            cursor = response.cursor
            if not cursor:
                break
        except AtProtocolError as e:
            print(f"❌ Could not fetch reposters: {e}")
            break
    print(f"  - Found {len(all_reposters)} reposters.")

    interactors = {user.did: user for user in all_liker_profiles + all_reposters}
    print(f"\nFound {len(interactors)} unique users to add to the blocklist.")

    added_count = 0
    skipped_count = 0
    failed_count = 0

    for did, user_profile in interactors.items():
        if did == client.me.did:
            continue

        try:
            # CHANGED: Replaced the model class with a direct dictionary definition
            # This is a more robust way to create the record.
            list_item_record = {
                '$type': 'app.bsky.graph.listitem',
                'subject': did,
                'list': list_uri,
                'createdAt': client.get_current_time_iso(),
            }

            client.com.atproto.repo.create_record(
                models.ComAtprotoRepoCreateRecord.Data(
                    repo=list_owner_did,
                    collection='app.bsky.graph.listitem',
                    record=list_item_record
                )
            )
            print(f"  [+] Added {user_profile.handle} ({did})")
            added_count += 1
        except AtProtocolError as e:
            if e.args and 'duplicate record' in str(e.args[0]):
                print(f"  [~] Skipped {user_profile.handle} (already in list)")
                skipped_count += 1
            else:
                print(f"  [!] Failed to add {user_profile.handle}. Reason: {e}")
                failed_count += 1

    print("\n--- Summary ---")
    print(f"✅ Successfully added: {added_count}")
    print(f"⚪ Skipped (already exists): {skipped_count}")
    print(f"❌ Failed: {failed_count}")
    print("✨ Process complete.")

if __name__ == "__main__":
    print("--- Bluesky Blocklist Automation Tool ---")
    print("This script will find everyone who liked or reposted a specific post")
    print("and add them to a blocklist you control.\n")

    post_url = os.environ.get("POST_URL") or input("Enter the URL of the target post: ")
    list_url = os.environ.get("LIST_URL") or input("Enter the URL of your blocklist: ")
    bsky_username = os.environ.get("BSKY_USERNAME") or input("Enter your Bluesky username (e.g., yourname.bsky.social): ")
    bsky_password = os.environ.get("BSKY_APP_PASSWORD") or getpass("Enter your Bluesky App Password: ")

    if not all([post_url, list_url, bsky_username, bsky_password]):
        print("\n❌ All fields are required. Exiting.")
        sys.exit(1)

    print("\nStarting process...")
    add_interactors_to_blocklist(post_url, list_url, bsky_username, bsky_password)