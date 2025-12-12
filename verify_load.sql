SELECT
    count(*) AS total_records_loaded,
    count(DISTINCT customer_id) AS unique_customer_ids,
    (
        SELECT
            count(*)
        FROM
            `de-project-479112.customer_data.customers`
        WHERE
            customer_name != INITCAP(customer_name)
    ) AS records_with_uncleaned_names,
    (
        SELECT
            count(*)
        FROM
            `de-project-479112.customer_data.customers`
        WHERE
            email != LOWER(email)
    ) AS records_with_uncleaned_emails
FROM
    `de-project-479112.customer_data.customers`