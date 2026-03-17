#Dict that maps columns to their tables
COLUMN_MAP ={
    # table: channel_and_user
    "user_name": "channel_and_user",


    # Table: input_type
    "input_type": "input_type",


    # Table: language
    "language": "language",


    # Table: month_wise_duration
    "month": "month_wise_duration",


    # Table: output_type
    "output_type": "output_type",


    # Table: channel_wise_publishing
    "reels":"channel_wise_publishing",
    "facebook":"channel_wise_publishing",
    "instagram":"channel_wise_publishing",
    "linkedin":"channel_wise_publishing",
    "reels":"channel_wise_publishing",
    "shorts":"channel_wise_publishing",
    "x":"channel_wise_publishing",
    "youtube":"channel_wise_publishing",
    "threads":"channel_wise_publishing",


    # Table: channel_wise_publishing_duration
    "reels_duration":"channel_wise_publishing_duration",
    "facebook_duration":"channel_wise_publishing_duration",
    "instagram_duration":"channel_wise_publishing_duration",
    "linkedin_duration":"channel_wise_publishing_duration",
    "reels_duration":"channel_wise_publishing_duration",
    "shorts_duration":"channel_wise_publishing_duration",
    "x_duration": "channel_wise_publishing_duration",
    "youtube_duration": "channel_wise_publishing_duration",
    "threads_duration": "channel_wise_publishing_duration",

    # Table: month_wise_duration
    "total_uploaded_duration": "month_wise_duration",
    "total_created_duration": "month_wise_duration",
    "total_published_duration": "month_wise_duration",

    # Table: monthly_chart
    "total_uploaded": "monthly_chart",
    "total_created": "monthly_chart",
    "total_published": "monthly_chart",

    # Table: video_list_data_obfuscated
    "headline": "video_list_data_obfuscated",
    "source": "video_list_data_obfuscated",
    "published": "video_list_data_obfuscated",
    "team_name": "video_list_data_obfuscated",
    "type": "video_list_data_obfuscated",
    "uploaded_by": "video_list_data_obfuscated",
    "video_id": "video_list_data_obfuscated",
    "published_platform": "video_list_data_obfuscated",
    "published_url": "video_list_data_obfuscated",

    # Table: video_list_data_synthesized  (row-level with timestamps)
    "duration_s": "video_list_data_synthesized",
    "created_ts": "video_list_data_synthesized",
    "published_ts": "video_list_data_synthesized",
}

# Common columns that appear in more than one table - cannot be used to uniquely identify a table
COMMON_COLUMNS = ["uploaded_count",
    "created_count",
    "published_count",
    "uploaded_duration",
    "created_duration",
    "published_duration",
    "channel",
    "month"]
