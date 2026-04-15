from googleapiclient.discovery import build


def get_youtube_client(api_key: str):
    """
    Initialize YouTube API client
    """
    return build("youtube", "v3", developerKey=api_key)


# ---------------------------
# CHANNEL DATA
# ---------------------------

def fetch_channel_data(youtube, channel_id: str):
    """
    Fetch channel metadata (snippet, statistics, contentDetails)
    """
    response = youtube.channels().list(
        part="snippet,statistics,contentDetails",
        id=channel_id
    ).execute()

    return response


def get_uploads_playlist_id(youtube, channel_id: str):
    """
    Get uploads playlist ID for a channel
    """
    response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


# ---------------------------
# VIDEO DATA
# ---------------------------

def fetch_playlist_videos(youtube, playlist_id: str):
    """
    Fetch all videos from a playlist (handles pagination)
    """
    videos = []
    next_page_token = None

    while True:
        response = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        videos.extend(response["items"])
        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break

    return videos


def fetch_video_statistics(youtube, video_ids: list):
    """
    Fetch video statistics + content details (including duration)
    """
    stats = []

    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="statistics,contentDetails",
            id=",".join(video_ids[i:i+50])
        ).execute()

        stats.extend(response.get("items", []))

    return stats