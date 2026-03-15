TABLE_NAME_PLACEHOLDER = "${TABLE_NAME}"
CREATE_USER_CDC_TABLE = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME_PLACEHOLDER} (
    uuid VARCHAR,
    read_ts TIMESTAMP,
    source_ts TIMESTAMP,
    object VARCHAR,
    read_method VARCHAR,
    stream_name VARCHAR,
    sort_keys JSON,
    source_metadata JSON,
    payload JSON,
    update_description JSON,
    raw_json JSON
);
"""
CDC_EVENT_FILE_PLACEHOLDER = "${CDC_EVENT_FILE}"
INSERT_USER_CDC_EVENTS = f"""
INSERT INTO {TABLE_NAME_PLACEHOLDER}
SELECT
    cdc_events.uuid,
    cdc_events.read_timestamp::TIMESTAMP AS read_ts,
    cdc_events.source_timestamp::TIMESTAMP AS source_ts,
    cdc_events.object,
    cdc_events.read_method,
    cdc_events.stream_name,
    cdc_events.sort_keys,
    cdc_events.source_metadata::JSON AS source_metadata,
    cdc_events.payload::JSON AS payload,
    json_extract(to_json(cdc_events), '$.update_description') AS update_description,
    to_json(cdc_events) AS raw_json
FROM
 read_json('{CDC_EVENT_FILE_PLACEHOLDER}', format='newline_delimited') cdc_events;
"""
SELECT_USER_TABLE_SNAPSHOT_WITH_HISTORY = f"""
WITH
parsed AS (
    SELECT
        payload->>'_id'                            AS _id,
        payload->>'firstname'                      AS firstname,
        payload->>'lastname'                       AS lastname,
        payload->>'email'                          AS email,
        payload->>'phone'                          AS phone,
        (payload->>'birthday')::DATE               AS birthday,
        payload->>'gender'                         AS gender,
        payload->'address'->>'street'              AS street,
        payload->'address'->>'city'                AS city,
        payload->'address'->>'zipcode'             AS zipcode,
        payload->'address'->>'country'             AS country,
        (payload->'address'->>'latitude')::DOUBLE  AS latitude,
        (payload->'address'->>'longitude')::DOUBLE AS longitude,
        payload->>'website'                        AS website,
        payload->>'image'                          AS image,
        source_ts,
        sort_keys,
        source_metadata->>'change_type'            AS change_type,
        (source_metadata->>'is_deleted')::BOOLEAN  AS is_deleted
    FROM {TABLE_NAME_PLACEHOLDER}
    WHERE object = 'users'
),

ordered AS (
    SELECT
        *,
        LEAD(source_ts) OVER (PARTITION BY _id ORDER BY sort_keys[0], sort_keys[1] ASC) AS next_ts
    FROM parsed
)

SELECT
    ROW_NUMBER() OVER (ORDER BY _id, sort_keys[0], sort_keys[1])    AS surrogate_key,
    _id,
    firstname,
    lastname,
    email,
    phone,
    birthday,
    gender,
    street,
    city,
    zipcode,
    country,
    latitude,
    longitude,
    website,
    image,
    change_type,
    is_deleted,
    sort_keys,
    sort_keys[0]                                    AS sort_key_1,
    sort_keys[1]                                    AS sort_key_2,
    source_ts                                       AS valid_from,
    COALESCE(next_ts, '9999-12-31'::TIMESTAMP)      AS valid_to,
    CASE
        WHEN next_ts IS NULL AND is_deleted = false  THEN true   -- latest record, not deleted
        WHEN next_ts IS NULL AND is_deleted = true   THEN false  -- latest record, but deleted
        ELSE false                                               -- historical record
    END                                             AS is_current
FROM ordered
ORDER BY _id, surrogate_key;
"""
SELECT_USER_TABLE_WITHOUT_PII = f"""
SELECT
        surrogate_key,
        _id,
        split_part(email, '@', 2) AS email_domain,
        CASE
            WHEN birthday IS NULL THEN NULL
            ELSE '[' ||
                (floor(date_diff('year', birthday, current_date) / 10) * 10)::INT
                || '-' ||
                (floor(date_diff('year', birthday, current_date) / 10) * 10 + 10)::INT
                || ']'
        END AS age_group,
        country,
        valid_from,
        valid_to,
        is_current,
        is_deleted
FROM {TABLE_NAME_PLACEHOLDER}
ORDER BY _id, surrogate_key;
"""
