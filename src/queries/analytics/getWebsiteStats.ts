import clickhouse from 'lib/clickhouse';
import { EVENT_TYPE } from 'lib/constants';
import { CLICKHOUSE, PRISMA, runQuery } from 'lib/db';
import prisma from 'lib/prisma';
import { QueryFilters } from 'lib/types';
import { EVENT_COLUMNS } from 'lib/constants';

export async function getWebsiteStats(...args: [websiteId: string, filters: QueryFilters]): Promise<
  {
    pageviews: number;
    visitors: number;
    visits: number;
    bounces: number;
    totaltime: number;
    conversions: number;
  }[]
> {
  return runQuery({
    [PRISMA]: () => relationalQuery(...args),
    [CLICKHOUSE]: () => clickhouseQuery(...args),
  });
}

async function relationalQuery(
  websiteId: string,
  filters: QueryFilters,
): Promise<
  {
    pageviews: number;
    visitors: number;
    visits: number;
    bounces: number;
    totaltime: number;
    conversions: number;
  }[]
> {
  const { getTimestampDiffSQL, parseFilters, rawQuery } = prisma;
  const { filterQuery, joinSession, params } = await parseFilters(websiteId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });
  const compiledRawQuery = `
    -- Step 1: Filter Data
    WITH filtered_events AS (
      SELECT website_event.* FROM website_event
        ${joinSession}
      WHERE website_event.website_id = {{websiteId::uuid}}
        AND website_event.created_at BETWEEN {{startDate}} AND {{endDate}}
        ${filterQuery}
    ),
    -- Step 2: Calculate main metrics
    metrics AS (
      SELECT
        filtered_events.session_id AS "session_id",
        filtered_events.visit_id AS "visit_id",
        COUNT(*) AS "c",
        MIN(filtered_events.created_at) AS "min_time",
        MAX(filtered_events.created_at) AS "max_time"
      FROM
        filtered_events
      WHERE
        filtered_events.event_type = {{eventType}} -- Filtering to pageview events.
      GROUP BY filtered_events.session_id, filtered_events.visit_id
    ),
    -- Step 3: Calculate conversions
    conversions AS (
      SELECT
        COUNT(DISTINCT filtered_events.event_id) AS "conversions"
      FROM
        filtered_events
      WHERE
        filtered_events.event_name = 'alert_submit'
    )
    -- Step 4: Bring everything together
    SELECT
      SUM(metrics.c) AS "pageviews",
      COUNT(DISTINCT metrics.session_id) AS "visitors",
      COUNT(DISTINCT metrics.visit_id) AS "visits",
      SUM(CASE WHEN metrics.c = 1 THEN 1 ELSE 0 END) AS "bounces",
      SUM(${getTimestampDiffSQL('metrics.min_time', 'metrics.max_time')}) AS "totaltime",
      (SELECT conversions.conversions FROM conversions) AS "conversions"
    FROM
      metrics
    `;
  return rawQuery(compiledRawQuery, params);
}

async function clickhouseQuery(
  websiteId: string,
  filters: QueryFilters,
): Promise<
  { pageviews: number; visitors: number; visits: number; bounces: number; totaltime: number }[]
> {
  const { rawQuery, parseFilters } = clickhouse;
  const { filterQuery, params } = await parseFilters(websiteId, {
    ...filters,
    eventType: EVENT_TYPE.pageView,
  });

  let sql = '';

  if (EVENT_COLUMNS.some(item => Object.keys(filters).includes(item))) {
    sql = `
    select
      sum(t.c) as "pageviews",
      uniq(t.session_id) as "visitors",
      uniq(t.visit_id) as "visits",
      sum(if(t.c = 1, 1, 0)) as "bounces",
      sum(max_time-min_time) as "totaltime"
    from (
      select
        session_id,
        visit_id,
        count(*) c,
        min(created_at) min_time,
        max(created_at) max_time
      from website_event
      where website_id = {websiteId:UUID}
        and created_at between {startDate:DateTime64} and {endDate:DateTime64}
        and event_type = {eventType:UInt32}
        ${filterQuery}
      group by session_id, visit_id
    ) as t;
    `;
  } else {
    sql = `
    select
      sum(t.c) as "pageviews",
      uniq(session_id) as "visitors",
      uniq(visit_id) as "visits",
      sumIf(1, t.c = 1) as "bounces",
      sum(max_time-min_time) as "totaltime"
    from (select
            session_id,
            visit_id,
            sum(views) c,
            min(min_time) min_time,
            max(max_time) max_time
        from umami.website_event_stats_hourly "website_event"
    where website_id = {websiteId:UUID}
      and created_at between {startDate:DateTime64} and {endDate:DateTime64}
      and event_type = {eventType:UInt32}
      ${filterQuery}
      group by session_id, visit_id
    ) as t;
    `;
  }

  return rawQuery(sql, params);
}
