create or replace package pkg_insider_yield as

  procedure run_yield
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_yield_revenue
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  );

  procedure load_yield_master
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  );

  procedure load_yield_report
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  );

  procedure load_preset_tables
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_min_date          number,
    p_max_date          number,
    p_table_name        varchar2,
    p_process_date_hour number
  );

  procedure load_homepage
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  );

  procedure manage_partitions
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  );

end;
/
create or replace package body pkg_insider_yield as

  procedure run_yield
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id            number := 15;
    l_process_date_hour number;
    l_process_name      varchar2(10);
    l_error_msg         varchar2(4000);
  begin
    l_process_name := initcap(p_process_name);
    if p_process_date <=
       to_number(to_char(sysdate - 1, 'yyyymmdd')) then
      -- find the last successful run for this date
      select to_number(to_char(to_date(to_char(coalesce(max(process_date), to_number(p_process_date || '00'))), 'yyyymmddhh24') +
                                1 / 24, 'yyyymmddhh24'))
      into   l_process_date_hour
      from   daily_process_log
      where  run_id = l_run_id
      and    process_date like p_process_date || '%'
      and    upper(process_name) = 'PROCESS-RUN_YIELD'
      and    status = 'COMPLETE';
      -- send intimation mail for starting of process
      --campaign.pkg_evolution.compose_sendemailmessage(l_run_id, 'START', l_process_date_hour, 'Yield Data', l_process_name);
      -- log starting of the process
      delete from campaign.daily_process_log
      where  process_date = l_process_date_hour
      and    run_id = l_run_id;
      commit;
      campaign.pkg_log_process.log_process(l_run_id, l_process_date_hour, l_process_name ||
                                            '-run_yield', 'I');
      load_yield_revenue(l_run_id, p_process_date, l_process_name, l_process_date_hour);
      if p_process_date <=
         to_number(to_char(last_day(add_months(sysdate - 1, -2)), 'yyyymmdd')) then
        load_yield_master(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        --load_homepage(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        -- mark the process as complete in log table
        campaign.pkg_log_process.log_process(l_run_id, l_process_date_hour, l_process_name ||
                                              '-run_yield', 'UC');
      elsif p_process_date <=
            to_number(to_char(last_day(add_months(sysdate - 1, -1)), 'yyyymmdd')) then
        load_yield_master(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        --load_homepage(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        -- mark the process as complete in log table
        campaign.pkg_log_process.log_process(l_run_id, l_process_date_hour, l_process_name ||
                                              '-run_yield', 'UC');
      else
        load_yield_master(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        --load_homepage(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        --load_yield_report(l_run_id, p_process_date, l_process_name, l_process_date_hour);
        -- mark the process as complete in log table
        campaign.pkg_log_process.log_process(l_run_id, l_process_date_hour, l_process_name ||
                                              '-run_yield', 'UC');
        --manage_partitions(l_run_id, p_process_date, l_process_name, l_process_date_hour);
      end if;
      -- send completion mail
      --campaign.pkg_evolution.compose_sendemailmessage(l_run_id, 'COMPLETE', l_process_date_hour, 'Yield Data', l_process_name);
    else
      campaign.pkg_log_process.log_process(l_run_id, l_process_date_hour, l_process_name, 'UF', 0, 'Wrong Date passed');
    end if;
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      campaign.pkg_log_process.log_process(l_run_id, l_process_date_hour, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
    --campaign.pkg_evolution.compose_sendemailmessage(l_run_id, 'FAIL', l_process_date_hour, 'Yield Data', l_process_name);
  end;

  procedure load_yield_revenue
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_str          varchar2(4000);
    l_substr_aff   varchar2(100);
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_aff_payments_nap';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      execute immediate 'truncate table ben_aff_payments_nap';
      for i in (select affiliate_id, rollup || ',' as rollup
                from   ap_processor.aff_payments@etlnap
                where  date_id =
                       to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))) loop
        l_str := i.rollup;
        while length(l_str) > 0 loop
          l_substr_aff := substr(l_str, 1, instr(l_str, ',', 1) - 1);
          l_str        := substr(l_str, instr(l_str, ',', 1) + 1);
          insert into ben_aff_payments_nap
            (affiliate_id, rollup)
          values
            (i.affiliate_id, l_substr_aff);
          commit;
        end loop;
      end loop;
      pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ben_yield_revenue_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      execute immediate 'Alter table ben_yield_revenue_new truncate partition P_' ||
                        substr(p_process_date, 1, 6);
      insert /*+ append */
      into ben_yield_revenue_new
        (month_id, date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed,
         ad_type, advertiser, affiliate_product_name,
         ad_category)
        with src as
         (select /*+ parallel(nrd,5,1) */
           coalesce(c.affiliate_id, nrd.affiliate_id) as affiliate_id,
           nrd.date_id, nrd.ad_id, nrd.country,
           nrd.impression_type,
           --sum(nrd.impressions_delivered) as impressions_delivered,
           case
             when nrd.ad_size_id <> '888x11' then
              sum(nrd.impressions_delivered)
             else
              0
           end as impressions_delivered,
           sum(nrd.clicks) as clicks, nrd.ad_size_id,
           nrd.ad_group_id,
           case
             when nrd.ad_size_id = '888x11' then
              sum(nrd.impressions_delivered)
             else
              0
           end imps_888_11,
           case
             when nrd.ad_size_id <> '888x11' then
              sum(nrd.impressions_delivered)
             else
              0
           end group_imps
          from   ben_yield_network_rev nrd,
                 ods_metadata.adm_ads b,
                 ben_aff_payments_nap c
          where  nrd.date_id >=
                 to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
          and    nrd.date_id <=
                 to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd'))
          and    category not in ('Ad Server', 'Internal')
          and    nrd.ad_id = b.ad_id
                --and    instr(rollup(+), nrd.affiliate_id) >= 1
          and    nrd.affiliate_id = c.rollup(+)
          group  by coalesce(c.affiliate_id, nrd.affiliate_id),
                    nrd.date_id, nrd.ad_id, nrd.country,
                    nrd.impression_type, nrd.ad_size_id,
                    nrd.ad_group_id),
        src2 as
         (select affiliate_id, date_id, country,
                 impression_type,
                 sum(imps_888_11) imps_888_11,
                 sum(group_imps) group_imps, ad_group_id
          from   src
          where  ad_group_id is not null
          group  by affiliate_id, date_id, country,
                    impression_type, ad_group_id),
        src3 as
         (select s.date_id, s.affiliate_id, s.ad_id,
                 s.country, s.impression_type, s.ad_size_id,
                 s.ad_group_id, sum(clicks) as clicks,
                 sum(s.impressions_delivered) impressions_delivered,
                 sum(case
                       when coalesce(s2.imps_888_11, 0) <> 0 then
                        impressions_delivered +
                        (s2.imps_888_11 * impressions_delivered /
                        s2.group_imps)
                       else
                        impressions_delivered
                     end) impressions_distributed
          from   src s, src2 s2
          where  s.date_id = s2.date_id(+)
          and    s.affiliate_id = s2.affiliate_id(+)
          and    s.country = s2.country(+)
          and    s.impression_type = s2.impression_type(+)
                --and    s.ad_group_id = s2.ad_group_id(+)
          and    coalesce(s.ad_group_id, -1) =
                 coalesce(s2.ad_group_id(+), -1)
          and    s.ad_size_id <> '888x11'
          group  by s.date_id, s.affiliate_id, s.ad_id,
                    s.country, s.impression_type,
                    s.ad_size_id, s.ad_group_id
          -- this is to include the rows which have clicks but no impressions(zero)
          union all
          select /*+ parallel(nrd,5,1) */
           nrd.date_id, nrd.affiliate_id, nrd.ad_id,
           nrd.country, nrd.impression_type, nrd.ad_size_id,
           nrd.ad_group_id, clicks,
           0 as impressions_delivered, 0
          from   ben_yield_clk_no_imps nrd
          where  date_id >=
                 to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
          and    date_id <=
                 to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd'))),
        src4 as
         (select s.date_id, s.affiliate_id, s.ad_id,
                 s.country, s.impression_type, s.ad_size_id,
                 s.ad_group_id, sum(clicks) as clicks,
                 sum(s.impressions_delivered) impressions_delivered,
                 sum(s.impressions_distributed) impressions_distributed
          from   src3 s
          group  by s.date_id, s.affiliate_id, s.ad_id,
                    s.country, s.impression_type,
                    s.ad_size_id, s.ad_group_id),
        oms_ads as
         (select pliad.ad_id, pdmad.ad_product_id,
                 pdmad.publisher_product_label as affiliate_product_name
          from   pdm_data.pdm_ad_line_item_ad_product@etlnap pliad,
                 pdm_data.pdm_ad_product@etlnap pdmad
          where  pliad.ad_product_id = pdmad.ad_product_id)
        select /*+ parallel(nrdp,4,1) */
         to_number(to_char(to_date(date_id, 'yyyymmdd'), 'yyyymm')) as month_id,
         date_id, affiliate_id, nrdp.ad_id, country,
         impression_type, nrdp.ad_size_id, ad_group_id,
         clicks, impressions_delivered,
         impressions_distributed,
         case
           when nae.category = 'Publisher Default Ads' and
                nrdp.impression_type = 'P' then
            'Premium Default Ads'
           when nae.category = 'Publisher Default Ads' and
                nrdp.impression_type = 'M' then
            'GlamX Default Ads'
           when nrdp.impression_type = 'M' then
            'GlamX'
           when nrdp.impression_type = 'P' then
            'Premium'
         end as ad_type, nae.advertiser_name as advertiser,
         coalesce(omsp.affiliate_product_name, 'Standard Media') as affiliate_product_name,
         case
           when nae.category = 'Advertiser' and
                nrdp.impression_type = 'P' then
            'Advertiser'
           when nae.category = 'Brand Research' then
            'Brand Research'
           when nae.category = 'Glam Advertising Partners' then
            'Glam Advertising Partners'
           when nae.category = 'Publisher Default Ads' and
                nae.sub_category = 'Self Supplied' then
            'Publisher Provided'
           when nae.category = 'Publisher Default Ads' and
                nae.sub_category = 'Glam Defaults' then
            'Glam House'
           when nae.category = 'Publisher Default Ads' and
                nae.sub_category = 'Branded' then
            'AwG'
           when nae.category = 'Publisher Default Ads' and
                nae.sub_category = 'PSA' then
            'PSA'
           when nae.category = 'Publisher Default Ads' and
                nae.sub_category = 'No Ad' then
            'No Ad'
           else
            nae.category
         end as ad_category
        from   src4 nrdp, ods_metadata.adm_ads nae,
               oms_ads omsp
        where  category not in ('Ad Server', 'Internal')
        and    nrdp.ad_id = omsp.ad_id(+)
        and    nrdp.ad_id = nae.ad_id(+);
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_yield_master
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_yield_master';
    if campaign.pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      for i in (select *
                from   user_tab_partitions
                where  table_name = 'BEN_YIELD_MASTER'
                and    partition_name like
                       '%' || substr(p_process_date, 1, 6) || '%'
                order  by partition_position) loop
        execute immediate 'Alter table ben_yield_master truncate partition ' ||
                          i.partition_name;
      end loop;
      insert /*+ append */
      into ben_yield_master
        (affiliate_id, country, ad_type, ad_size,
         advertiser, date_id, impressions_delivered, clicks,
         preliminary_earnings, affiliate_product_name,
         ad_category)
        with apd_final as
         (select apd.affiliate_id, ad_id,
                 (case line_type
                    when 'PremiumDetail' then
                     'P'
                    when 'GlamxDetail' then
                     'M'
                  end) as impression_type,
                 impressions_delivered,
                 case
                   when impressions_delivered = 0 then
                    0
                   else
                    coalesce(((apd.affiliate_earnings /
                             apd.impressions_delivered)), 0)
                 end as earnings_per_ad_per_imp
          --from   ap_data.affiliate_payments_detail@nap apd
          from   ap_processor.aff_payments_detail@etlnap apd
          where  date_id =
                 to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
          and    apd.statement_period =
                 to_char(to_date(p_process_date, 'yyyymmdd'), 'mm/yyyy')
          and    line_type != 'PremiumAd')
        select nrdp.affiliate_id, nrdp.country, ad_type,
               nrdp.ad_size_id, advertiser, nrdp.date_id,
               nrdp.impressions_delivered, nrdp.clicks,
               coalesce(earnings_per_ad_per_imp *
                         nrdp.impressions_distributed, 0) as preliminary_earnings,
               nrdp.affiliate_product_name, ad_category
        from   ben_yield_revenue_new nrdp, apd_final apdp
        where  month_id = substr(p_process_date, 1, 6)
        and    nrdp.ad_id = apdp.ad_id(+)
        and    nrdp.affiliate_id = apdp.affiliate_id(+)
        and    nrdp.impression_type =
               apdp.impression_type(+);
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
    --
    l_process_name := p_process_name || '-' ||
                      'ben_yield_custom';
    if campaign.pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      for i in (select *
                from   user_tab_partitions
                where  table_name = 'BEN_YIELD_CUSTOM'
                and    partition_name like
                       '%' || substr(p_process_date, 1, 6) || '%'
                order  by partition_position) loop
        execute immediate 'Alter table ben_yield_custom truncate partition ' ||
                          i.partition_name;
      end loop;
      insert /*+ append */
      into ben_yield_custom
        (date_id, affiliate_id, country, ad_type, ad_size,
         impressions_delivered, clicks, preliminary_earnings,
         impressions_sold /*, fill_rate, ctr, ecpm*/)
        select /*+ parallel(a,4) */
         date_id, affiliate_id, country,
         case
           when ad_type = 'Premium Default Ads' then
            'Premium'
           when ad_type = 'GlamX Default Ads' then
            'GlamX'
           else
            ad_type
         end as ad_type, ad_size, sum(impressions_delivered),
         sum(clicks), sum(preliminary_earnings),
         sum(case
               when ad_type not in
                    ('Premium Default Ads', 'GlamX Default Ads') then
                impressions_delivered
               else
                0
             end) as impressions_sold
        from   ben_yield_master a
        where  date_id >=
               to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
        and    date_id <= p_process_date
        group  by date_id, affiliate_id, country,
                  case
                    when ad_type = 'Premium Default Ads' then
                     'Premium'
                    when ad_type = 'GlamX Default Ads' then
                     'GlamX'
                    else
                     ad_type
                  end, ad_size;
      l_cnt := sql%rowcount;
      commit;
      for x in (select *
                from   user_tab_partitions
                where  table_name = 'BEN_YIELD_CUSTOM'
                and    partition_name like
                       '%' || substr(p_process_date, 1, 6) || '%'
                order  by partition_position) loop
        execute immediate 'analyze table ben_yield_custom partition (' ||
                          x.partition_name ||
                          ') estimate statistics';
      end loop;
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_yield_report
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  ) as
    l_7_days_min  number;
    l_month_start number;
    l_30_days_min number;
  begin
    -- yield last 7 days
    l_7_days_min := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 6, 'yyyymmdd'));
    -- yield month to date
    l_month_start := to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'));
    -- yield last 30 days
    l_30_days_min := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 29, 'yyyymmdd'));
    ---- Preset Tables
    for x in (select case
                       when level = 1 then
                        'YIELD'
                       when level = 2 then
                        'REVENUE'
                       when level = 3 then
                        'ADUNIT'
                       when level = 4 then
                        'ADVERTISER'
                       when level = 5 then
                        'COUNTRY'
                     end as table_alias
              from   dual
              connect by level <= 5) loop
      for y in (select level as table_id,
                       case
                         when level = 1 then
                          x.table_alias || '_LAST_7_DAYS'
                         when level = 2 then
                          x.table_alias || '_LAST_30_DAYS'
                         when level = 3 then
                          x.table_alias || '_MONTH_TO_DATE'
                       end as table_name,
                       case
                         when level = 1 then
                          l_7_days_min
                         when level = 2 then
                          l_30_days_min
                         when level = 3 then
                          l_month_start
                       end as min_date,
                       p_process_date as max_date
                from   dual
                connect by level <= 3) loop
        load_preset_tables(p_run_id, p_process_date, p_process_name, y.min_date, y.max_date, y.table_name, p_process_date_hour);
      end loop;
    end loop;
  end;

  procedure load_preset_tables
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_min_date          number,
    p_max_date          number,
    p_table_name        varchar2,
    p_process_date_hour number
  ) as
    l_process_name   varchar2(100);
    l_cnt            number := 0;
    l_yesterday_date number;
  begin
    l_yesterday_date := to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 1, 'yyyymmdd'));
    l_process_name   := p_process_name || '-' ||
                        p_table_name;
    if campaign.pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      if upper(p_table_name) like 'YIELD%' then
        -- need to add the script for yesterday with column addition as no_of_days
        execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (date_id, affiliate_id, country, ad_type, ad_size,
             impressions_delivered, clicks,
             preliminary_earnings, affiliate_product_name,
             process_date, impressions_sold)
            select /*+ parallel(a,4) */
             date_id, affiliate_id, country,
             case when ad_type = ''Premium Default Ads'' then
                    ''Premium''
                  when ad_type = ''GlamX Default Ads'' then
                    ''GlamX''
                  else
                    ad_type
             end as ad_type, ad_size,
             sum(impressions_delivered), sum(clicks),
             sum(preliminary_earnings), affiliate_product_name,' ||
                          p_process_date_hour || ',
             sum(case
                   when ad_type not in
                        (''Premium Default Ads'', ''GlamX Default Ads'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_sold
            from   ben_yield_master a
            where  date_id >= ' ||
                          p_min_date || '
            and    date_id <= ' ||
                          p_max_date || '
            group  by date_id, affiliate_id, country,
                      case when ad_type = ''Premium Default Ads'' then
                             ''Premium''
                           when ad_type = ''GlamX Default Ads'' then
                             ''GlamX''
                           else
                             ad_type
                      end, ad_size, affiliate_product_name';
      elsif upper(p_table_name) like 'REVENUE%' then
        execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (date_id, affiliate_id, impressions_available,
             impressions_used, sell_through, premium_revenue,
             other_revenue, total_revenue, total_ecpm, process_date)
            select /*+ parallel(a,4,1) */
             date_id, affiliate_id, impressions_available,
             impressions_used,
             case
               when impressions_available = 0 then
                0
               else
                (impressions_used /
                  impressions_available)* 100
             end as sell_through, premium_revenue,
             other_revenue, total_revenue,
             case
               when impressions_used = 0 then
                0
               else
                total_revenue /
                  (impressions_used / 1000)
             end as total_ecpm,' ||
                          p_process_date_hour || '
            from (
            select /*+ parallel(a,4,1) */
             date_id, affiliate_id,
             sum(impressions_delivered) as impressions_available,
             sum(case
                   when ad_type not in
                        (''Premium Default Ads'', ''GlamX Default Ads'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_used,
             sum(case
                   when ad_type = ''Premium'' then
                    preliminary_earnings
                   else
                    0
                 end) as premium_revenue,
             sum(case
                   when ad_type <> ''Premium'' then
                    preliminary_earnings
                   else
                    0
                 end) as other_revenue,
             sum(preliminary_earnings) as total_revenue
            from   ben_yield_master a
            where  date_id >= ' ||
                          p_min_date || '
            and    date_id <= ' ||
                          p_max_date || '
            group  by date_id, affiliate_id)';
      elsif upper(p_table_name) like 'ADUNIT%' then
        execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (date_id, affiliate_id, ad_size,
             impressions_available_premium, impressions_sold_premium,
             revenue_premium, impressions_available_other,
             impressions_sold_other, revenue_other,
             impressions_available_total, impressions_sold_total,
             revenue_total, process_date)
            select /*+ parallel(a,4,1) */
             date_id, affiliate_id, ad_size,
             sum(case
                   when ad_type in (''Premium'', ''Premium Default Ads'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_available_premium,
             sum(case
                   when ad_type in (''Premium'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_sold_premium,
             sum(case
                   when ad_type in (''Premium'') then
                    preliminary_earnings
                   else
                    0
                 end) as revenue_premium,
             sum(case
                   when ad_type not in
                        (''Premium'', ''Premium Default Ads'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_available_other,
             sum(case
                   when ad_type not in
                        (''Premium'', ''Premium Default Ads'', ''GlamX Default Ads'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_sold_other,
             sum(case
                   when ad_type not in
                        (''Premium'', ''Premium Default Ads'', ''GlamX Default Ads'') then
                    preliminary_earnings
                   else
                    0
                 end) as revenue_other,
             sum(impressions_delivered) as impressions_available_total,
             sum(case
                   when ad_type not in
                        (''Premium Default Ads'', ''GlamX Default Ads'') then
                    impressions_delivered
                   else
                    0
                 end) as impressions_sold_total,
             sum(case
                   when ad_type not in
                        (''Premium Default Ads'', ''GlamX Default Ads'') then
                    preliminary_earnings
                   else
                    0
                 end) as revenue_total,' ||
                          p_process_date_hour || '
            from   (select affiliate_id, ad_type,
                           case
                             when ad_size = ''120x600'' then
                              ''160x600''
                             when ad_size = ''300x600'' then
                              ''300x250''
                             else
                              ad_size
                           end as ad_size, date_id, impressions_delivered,
                           clicks, preliminary_earnings
                    from   ben_yield_master a
                    where  date_id >= ' ||
                          p_min_date || '
                    and    date_id <= ' ||
                          p_max_date || '
                    and    ad_size in
                         (''728x90'',''160x600'',''120x600'',''300x250'',''300x600'',''320x50'',''970x66'')
                    union all
                    select affiliate_id, ad_type, ad_size,
                           date_id, impressions_delivered,
                           clicks, preliminary_earnings
                    from   yield_970x66_adserver
                    where  date_id >= ' ||
                          p_min_date || '
                    and    date_id <= ' ||
                          p_max_date || ') a
            group  by date_id, affiliate_id, ad_size';
      elsif upper(p_table_name) like 'ADVERTISER%' then
        if upper(p_table_name) = 'ADVERTISER_LAST_7_DAYS' then
          execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (affiliate_id, advertiser, product_name,
             impressions_available, clicks, ctr, publisher_cpm,
             total_earnings, no_of_days, process_date)
            select /*+ parallel(a,4,1) */
             affiliate_id, advertiser, affiliate_product_name,
             sum(impressions_delivered) as impressions_available,
             sum(clicks) as clicks,
             case
               when sum(impressions_delivered) = 0 then
                0
               else
                sum(clicks) * 100 / sum(impressions_delivered)
             end as ctr,
             case
               when sum(impressions_delivered) = 0 then
                0
               else
                 sum(preliminary_earnings) /
                  (sum(impressions_delivered) / 1000)
             end as publisher_cpm,
             sum(preliminary_earnings) as total_earning,
             1 as no_of_days, ' ||
                            p_process_date_hour || '
            from   ben_yield_master a
            where  date_id = ' ||
                            l_yesterday_date || '
            and    advertiser is not null
            and    ad_type in (''Premium'')
            and    ad_category = ''Advertiser''
            group  by affiliate_id, advertiser, affiliate_product_name';
          commit;
        end if;
        execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (affiliate_id, advertiser, product_name,
             impressions_available, clicks, ctr, publisher_cpm,
             total_earnings, no_of_days, process_date)
            select /*+ parallel(a,4,1) */
             affiliate_id, advertiser, affiliate_product_name,
             sum(impressions_delivered) as impressions_available,
             sum(clicks) as clicks,
             case
               when sum(impressions_delivered) = 0 then
                0
               else
                sum(clicks) * 100 / sum(impressions_delivered)
             end as ctr,
             case
               when sum(impressions_delivered) = 0 then
                0
               else
                 sum(preliminary_earnings) /
                  (sum(impressions_delivered) / 1000)
             end as publisher_cpm,
             sum(preliminary_earnings) as total_earning,
             7 as no_of_days, ' ||
                          p_process_date_hour || '
            from   ben_yield_master a
            where  date_id >= ' ||
                          p_min_date || '
            and    date_id <= ' ||
                          p_max_date || '
            and    advertiser is not null
            and    ad_type in (''Premium'')
            and    ad_category = ''Advertiser''
            group  by affiliate_id, advertiser, affiliate_product_name';
      elsif upper(p_table_name) like 'COUNTRY%' then
        if upper(p_table_name) = 'COUNTRY_LAST_7_DAYS' then
          execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (affiliate_id, country, impressions_available,
             percentage_impressions, impressions_sold, sell_through,
             revenue, percentage_revenue, ecpm, no_of_days,
             process_date)
            with src as
             (select /*+ parallel(a,4,1) */
               affiliate_id,
               sum(impressions_delivered) as impressions_ww,
               sum(preliminary_earnings) as revenue_ww
              from   ben_yield_master a
              where  date_id = ' ||
                            l_yesterday_date || '
              group  by affiliate_id),
            yield_agg as
             (select /*+ parallel(a,4,1) */
               affiliate_id, country,
               sum(impressions_delivered) as impressions_available,
               sum(case
                     when ad_type not in
                          (''Premium Default Ads'', ''GlamX Default Ads'') then
                      impressions_delivered
                     else
                      0
                   end) as impressions_sold,
               sum(case
                     when ad_type not in
                          (''Premium Default Ads'', ''GlamX Default Ads'') then
                      preliminary_earnings
                     else
                      0
                   end) as revenue
              from   ben_yield_master a
              where  date_id = ' ||
                            l_yesterday_date || '
              group  by a.affiliate_id, country)
            select a.affiliate_id, country, impressions_available,
                   case
                     when impressions_ww = 0 then
                       0
                     else
                       (impressions_available / impressions_ww) * 100
                   end as percentage_impressions,
                   impressions_sold,
                   case
                     when impressions_available = 0 then
                       0
                     else
                       (impressions_sold / impressions_available) * 100
                   end as sell_through,
                   revenue,
                   case
                     when revenue_ww > 0 then
                      (revenue / revenue_ww) * 100
                     else
                      0
                   end as percentage_revenue,
                   case
                     when impressions_sold > 0 then
                      revenue / (impressions_sold / 1000)
                     else
                      0
                   end as ecpm, 1 as no_of_days, ' ||
                            p_process_date_hour || '
            from   yield_agg a, src b
            where  a.affiliate_id = b.affiliate_id';
          commit;
        end if;
        execute immediate '
          insert /*+ append */
          into ' || p_table_name || '
            (affiliate_id, country, impressions_available,
             percentage_impressions, impressions_sold, sell_through,
             revenue, percentage_revenue, ecpm, no_of_days,
             process_date)
            with src as
             (select /*+ parallel(a,4,1) */
               affiliate_id,
               sum(impressions_delivered) as impressions_ww,
               sum(preliminary_earnings) as revenue_ww
              from   ben_yield_master a
              where  date_id >= ' ||
                          p_min_date || '
              and    date_id <= ' ||
                          p_max_date || '
              group  by affiliate_id),
            yield_agg as
             (select /*+ parallel(a,4,1) */
               affiliate_id, country,
               sum(impressions_delivered) as impressions_available,
               sum(case
                     when ad_type not in
                          (''Premium Default Ads'', ''GlamX Default Ads'') then
                      impressions_delivered
                     else
                      0
                   end) as impressions_sold,
               sum(case
                     when ad_type not in
                          (''Premium Default Ads'', ''GlamX Default Ads'') then
                      preliminary_earnings
                     else
                      0
                   end) as revenue
              from   ben_yield_master a
              where  date_id >= ' ||
                          p_min_date || '
              and    date_id <= ' ||
                          p_max_date || '
              group  by a.affiliate_id, country)
            select a.affiliate_id, country, impressions_available,
                   case
                     when impressions_ww = 0 then
                       0
                     else
                       (impressions_available / impressions_ww) * 100
                   end as percentage_impressions,
                   impressions_sold,
                   case
                     when impressions_available = 0 then
                       0
                     else
                       (impressions_sold / impressions_available) * 100
                   end as sell_through,
                   revenue,
                   case
                     when revenue_ww > 0 then
                      (revenue / revenue_ww) * 100
                     else
                      0
                   end as percentage_revenue,
                   case
                     when impressions_sold > 0 then
                      revenue / (impressions_sold / 1000)
                     else
                      0
                   end as ecpm, 7 as no_of_days, ' ||
                          p_process_date_hour || '
            from   yield_agg a, src b
            where  a.affiliate_id = b.affiliate_id';
      end if;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_homepage
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
    l_month_start  number;
    l_month_end    number;
  begin
    l_month_start  := to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'));
    l_month_end    := to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd'));
    l_process_name := p_process_name || '-' ||
                      'new_insider_home';
    if campaign.pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      delete from insider_data.new_insider_home_yield@etlwww
      where  date_id >= l_month_start
      and    date_id <= l_month_end;
      insert into insider_data.new_insider_home_yield@etlwww
        (affiliate_id, date_id, revenue, ecpm, chart_type,
         preliminary_total_earnings, impressions_delivered,
         total_available, total_sold, graph_type)
        select /*+ parallel(a,4) */
         affiliate_id, date_id,
         sum(preliminary_earnings) as revenue,
         case
           when sum(impressions_delivered) = 0 then
            0
           else
            sum(preliminary_earnings) /
            (sum(impressions_delivered) / 1000)
         end as ecpm, 'PREMIUM_HI' chart_type,
         sum(preliminary_earnings) preliminary_total_earnings,
         sum(impressions_delivered) as impressions_delivered,
         0, 0, 'REVENUE' as graph_type
        from   ben_yield_master a
        where  date_id >= l_month_start
        and    date_id <= l_month_end
        and    ad_type not in
               ('Brand Research', 'Glam Advertising Partners', 'Premium Default Ads', 'GlamX Default Ads', 'GlamX')
        and    ad_size in
               ('160x600', '728x90', '300x250', '970x66')
        group  by affiliate_id, date_id
        union all
        select /*+ parallel(a,4) */
         affiliate_id, date_id,
         sum(preliminary_earnings) as revenue,
         case
           when sum(impressions_delivered) = 0 then
            0
           else
            sum(preliminary_earnings) /
            (sum(impressions_delivered) / 1000)
         end as ecpm, 'OTHER' chart_type,
         sum(preliminary_earnings) preliminary_total_earnings,
         sum(impressions_delivered) as impressions_delivered,
         0, 0, 'REVENUE' as graph_type
        from   ben_yield_master a
        where  date_id >= l_month_start
        and    date_id <= l_month_end
        and    ad_type not in ('Premium')
        and    ad_size in
               ('160x600', '728x90', '300x250', '970x66')
        group  by affiliate_id, date_id
        union all
        select /*+ parallel(a,4) */
         affiliate_id, date_id, 0, 0, '-1', 0, 0,
         sum(impressions_delivered) as total_available,
         sum(case
               when ad_type not in
                    ('Premium Default Ads', 'GlamX Default Ads') then
                impressions_delivered
               else
                0
             end) as total_sold, 'IMPRESSIONS' as graph_type
        from   ben_yield_master a
        where  date_id >= l_month_start
        and    date_id <= l_month_end
        and    ad_size in ('160x600', '728x90', '300x250')
        group  by affiliate_id, date_id
        order  by affiliate_id desc;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure manage_partitions
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  ) as
    l_process_name varchar2(10);
    l_status_date  number;
    l_part_name    varchar2(1000);
  begin
    -- delete old daily process log records from the table
    delete from campaign.daily_process_log
    where  process_date <
           to_number(to_char(to_date(p_process_date, 'yyyymmdd') - 2, 'yyyymmddhh24'))
    and    run_id = p_run_id;
    commit;
    -- call the procedure to drop the old partitions from the yield tables
    select wm_concat(process_date)
    into   l_part_name
    from   (select process_date,
                    rank() over(partition by status order by process_date desc) rank
             from   daily_process_log
             where  run_id = p_run_id
             and    upper(process_name) = 'PROCESS-RUN_YIELD'
             and    status = 'COMPLETE')
    where  rank <= 2;
    --call the proce from www to drop the partitions
    -- update the yield status table
    l_process_name := p_process_name || '-' ||
                      'yield_status@etlwww';
    if campaign.pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      select process_date
      into   l_status_date
      from   (select process_date,
                      rank() over(partition by status order by process_date desc) rank
               from   daily_process_log
               where  run_id = p_run_id
               and    upper(process_name) =
                      'PROCESS-RUN_YIELD'
               and    status = 'COMPLETE')
      where  rank = 1;
      update insider_data.yield_status@etlwww
      set    process_date = l_status_date,
             process_day = 'TODAY'
      where  process_day = 'YESTERDAY';
      update insider_data.yield_status@etlwww
      set    process_day = 'YESTERDAY'
      where  process_date <> l_status_date;
      commit;
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', 1);
    end if;
  end;

  procedure load_yield_custom
  (
    p_run_id            number,
    p_process_date      number,
    p_process_name      varchar2,
    p_process_date_hour number
  ) as
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'yield_custom@etlwww';
    if campaign.pkg_log_process.is_not_complete(p_run_id, p_process_date_hour, l_process_name) then
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'I');
      delete from insider_data.yield_custom@etlwww
      where  date_id >=
             to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
      and    date_id <= p_process_date;
      insert into insider_data.yield_custom@etlwww
        (date_id, affiliate_id, country, ad_type, ad_size,
         impressions_delivered, clicks, preliminary_earnings,
         impressions_sold /*, fill_rate, ctr, ecpm*/)
        select /*+ parallel(a,4) */
         date_id, affiliate_id, country, ad_type, ad_size,
         impressions_delivered, clicks, preliminary_earnings,
         impressions_sold /*, fill_rate, ctr, ecpm*/
        from   ben_yield_custom a
        where  date_id >=
               to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
        and    date_id <= p_process_date;
      l_cnt := sql%rowcount;
      commit;
      campaign.pkg_log_process.log_process(p_run_id, p_process_date_hour, l_process_name, 'UC', l_cnt);
    end if;
  end;

end;
/
