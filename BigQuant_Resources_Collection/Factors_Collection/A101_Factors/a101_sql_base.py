def get_a101_sql_base(sql_alpha):
    
    sql_base = f"""--sql
    WITH
    data_base AS (
        SELECT
            date,
            instrument,
            open,
            close,
            high,
            low,
            amount,
            volume,
            close / m_lag(close, 1) - 1 AS returns,
            amount / volume             AS vwap,
            m_nanavg(volume,   5)       AS adv5,
            m_nanavg(volume,  10)       AS adv10,
            m_nanavg(volume,  15)       AS adv15,
            m_nanavg(volume,  20)       AS adv20,
            m_nanavg(volume,  30)       AS adv30,
            m_nanavg(volume,  40)       AS adv40,
            m_nanavg(volume,  50)       AS adv50,
            m_nanavg(volume,  60)       AS adv60,
            m_nanavg(volume,  81)       AS adv81,
            m_nanavg(volume, 120)       AS adv120,
            m_nanavg(volume, 150)       AS adv150,
            m_nanavg(volume, 180)       AS adv180,
            float_market_cap            AS cap,
            sw2021_level1               AS class_lv1,
            sw2021_level2               AS class_lv2,
            sw2021_level3               AS class_lv3,
        FROM cn_stock_prefactors 
        QUALIFY COLUMNS(*) IS NOT NULL
        ORDER BY date, instrument
    ),
    data_factor AS (
        {sql_alpha}
    ),
    data_security AS (
        SELECT
            date,
            instrument,
        FROM cn_stock_instruments
    ),
    data_merge AS (
        SELECT *
        FROM data_security LEFT JOIN data_factor USING (date, instrument)
    )
    SELECT *
    FROM data_merge
    ORDER BY date, instrument
    """

    return sql_base
