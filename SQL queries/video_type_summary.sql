SELECT
  video_type,
  total_videos,
  total_views,
  avg_views,
  avg_engagement
FROM workspace_yt_mar.gold.video_type_summary 
ORDER BY video_type LIMIT 10;