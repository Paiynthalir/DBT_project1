{% snapshot review_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='inserted_datetime'
        )
    }}

select listing_id, SCRAPED_DATE, NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE, inserted_datetime from {{ source('raw', 'listings') }}

{% endsnapshot %}