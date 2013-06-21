create or replace package pkg_insider_data as

  procedure run_insider
  (
    p_process_name varchar2,
    p_process_date number
  );

  procedure load_ben_yield_nrd
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ben_yield_staging
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ben_yield_master_stg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_ben_traffic_revenue
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure replicate_to_www
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end;
/
create or replace package body pkg_insider_data as

  procedure run_insider
  (
    p_process_name varchar2,
    p_process_date number
  ) is
    l_run_id        number;
    l_process_date  number;
    l_process_name  varchar2(10);
    l_error_msg     varchar2(4000);
    l_start_process varchar2(100);
    l_mail_msg      varchar2(100);
    l_to            varchar2(1000);
  begin
    l_process_name  := initcap(p_process_name);
    l_start_process := l_process_name || '-run_insider';
    l_mail_msg      := 'ODS INSIDER Data Process';
    l_to            := 'nileshm@glam.com,sahilt@glam.com';
    if p_process_date is null then
      l_process_date := to_number(to_char(sysdate - 1, 'yyyymmdd'));
    elsif p_process_date is not null then
      l_process_date := p_process_date;
    end if;
    select max(run_id)
    into   l_run_id
    from   daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process);
    if l_run_id is null then
      l_run_id := seq_daily_campaign_run.nextval;
    end if;
    -- make sure we delete the record for the main process name(this has dependency in shell script).
    delete from daily_process_log
    where  process_date = l_process_date
    and    upper(process_name) = upper(l_start_process)
    and    run_id = l_run_id
    and    status in ('RUNNING', 'FAIL');
    commit;
    -- send intimation mail for starting of process
    --pkg_compose_email.run_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    pa_send_email1.compose_email(l_run_id, 'START', l_process_date, l_mail_msg, l_start_process, l_to);
    -- log starting of the process
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'I');
    load_ben_yield_nrd(l_run_id, l_process_date, l_process_name);
    load_ben_yield_staging(l_run_id, l_process_date, l_process_name);
    --load_ben_yield_revenue(l_run_id, l_process_date, l_process_name);
    load_ben_yield_master_stg(l_run_id, l_process_date, l_process_name);
    load_ben_traffic_revenue(l_run_id, l_process_date, l_process_name);
    replicate_to_www(l_run_id, l_process_date, l_process_name);
    -- mark the process as complete in log table
    pkg_log_process.log_process(l_run_id, l_process_date, l_start_process, 'UC');
    -- send completion mail
    --pkg_compose_email.run_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
    pa_send_email1.compose_email(l_run_id, 'COMPLETE', l_process_date, l_mail_msg, l_start_process, l_to);
  exception
    when others then
      l_error_msg := dbms_utility.format_error_backtrace ||
                     dbms_utility.format_error_stack;
      pkg_log_process.log_process(l_run_id, l_process_date, '', 'UF', 0, l_error_msg);
      -- failed steps are handled in compose mail message procedure
      --pkg_compose_email.run_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
      pa_send_email1.compose_email(l_run_id, 'FAIL', l_process_date, l_mail_msg, l_start_process, l_to);
  end;

  procedure load_ben_yield_nrd
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_yield_nrd';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_yield_nrd truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_yield_nrd
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, inventory_impressions,
         order_id)
        select /*+ parallel(nrd,5,1) */
         date_id, affiliate_id, nrd.ad_id, country,
         impression_type, b.ad_size_id, ad_group_id,
         sum(clicks) as clicks,
         sum(impressions_delivered) as impressions_delivered,
         sum(total_inventory) as inventory_impressions,
         order_id
        from   ben_network_revenue_daily nrd,
               ods_metadata.adm_ads b
        where  date_id = p_process_date
        and    country <> 'WW'
        and    nrd.impression_type in ('P', 'M')
        and    nrd.ad_id = b.ad_id
        and    ad_size_id in
               ('160x600', '120x600', '300x250', '300x600', '728x90', '555x2', '320x50', '300x50', '888x11', '970x66', '970x250', '888x12', '280x330', '888x23')
        group  by date_id, affiliate_id, nrd.ad_id, country,
                  impression_type, b.ad_size_id, ad_group_id,
                  order_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_nrd partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ben_yield_nrd_content_partner';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      --no truncate partition, since it is handled in previous step, this is just insert
      insert /*+ append */
      into ben_yield_nrd
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, inventory_impressions,
         order_id)
        select /*+ parallel(nrd,5,1) */
         date_id, content_partner_id, nrd.ad_id, nrd.country,
         'P' as impression_type, b.ad_size_id, ad_group_id,
         sum(clicks) as clicks,
         sum(impressions_delivered) as impressions_delivered,
         sum(total_inventory) as inventory_impressions,
         order_id
        from   ben_network_revenue_daily nrd,
               ods_metadata.adm_ads b,
               ods_metadata.channel c
        where  date_id = p_process_date
        and    nrd.country <> 'WW'
        and    nrd.impression_type in ('A', 'G', 'P')
        and    nrd.ad_id = b.ad_id
        and    ad_size_id in
               ('160x600', '120x600', '300x250', '300x600', '728x90', '555x2', '320x50', '300x50', '888x11', '970x66', '970x250', '888x12', '280x330', '888x23')
        and    content_partner_id = id
        and    affiliate_type = 'Content Partner'
        group  by date_id, content_partner_id, nrd.ad_id,
                  nrd.country, b.ad_size_id, ad_group_id,
                  order_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_nrd partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ben_yield_staging
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_yield_network_rev';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_yield_network_rev truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into campaign.ben_yield_network_rev
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed)
        select /*+ parallel(a,5,1) */
         date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id,
         sum(clicks) as clicks,
         sum(impressions_delivered) as impressions_delivered,
         0
        from   ben_yield_nrd a
        where  date_id = p_process_date
        and    impressions_delivered > 0
        group  by date_id, affiliate_id, ad_id, country,
                  impression_type, ad_size_id, ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_network_rev partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ben_yield_clk_no_imps';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_yield_clk_no_imps truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into campaign.ben_yield_clk_no_imps
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed)
        select /*+ parallel(a,5,1) */
         date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id,
         sum(clicks) as clicks, 0, 0
        from   ben_yield_nrd a
        where  date_id = p_process_date
        and    impressions_delivered = 0
        and    clicks > 0
        group  by date_id, affiliate_id, ad_id, country,
                  impression_type, ad_size_id, ad_group_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_clk_no_imps partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- ben_yield_970x66_adserver
    l_process_name := p_process_name || '-' ||
                      'ben_yield_970x66_adserver';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_yield_970x66_adserver_1 truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_yield_970x66_adserver_1
        (date_id, affiliate_id, ad_type, ad_size,
         impressions_delivered, clicks, preliminary_earnings,
         country, advertiser, affiliate_product_name,
         ad_category, ad_id)
      /* with oms_ads as
      (select pliad.ad_id, pdmad.ad_product_id,
              pdmad.publisher_product_label as affiliate_product_name
       from   pdm_data.pdm_ad_line_item_ad_product@nap pliad,
              pdm_data.pdm_ad_product@nap pdmad
       where  pliad.ad_product_id = pdmad.ad_product_id)*/
        select /*+ parallel(a,5,1) */
         date_id, affiliate_id,
         case
           when impression_type = 'P' then
            'Premium Default Ads'
           when impression_type = 'M' then
            'GlamX Default Ads'
         end, a.ad_size_id,
         sum(impressions_delivered) impressions_delivered,
         sum(clicks) as clicks, 0 as preliminary_earnings,
         country, advertiser_name, affiliate_product_name,
         'Publisher Provided' as ad_category, a.ad_id
        from   ben_yield_nrd a, ods_metadata.adm_ads b,
               ben_oms_pdm_ad_product c
        where  date_id = p_process_date
        and    impressions_delivered > 0
        and    category in ('Ad Server')
        and    a.ad_size_id in ('970x66')
        and    a.ad_id = b.ad_id
        and    a.ad_id = c.ad_id(+)
        group  by date_id, affiliate_id, impression_type,
                  a.ad_size_id, country, advertiser_name,
                  affiliate_product_name, a.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_970x66_adserver_1 partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ben_yield_master_stg
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_yield_master_stg1';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_yield_master_stg1';
      -- this is to include the rows which have clicks but no impressions(zero)
      insert /*+ append */
      into ben_yield_master_stg1
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, imps_888_11, group_imps,
         distribution_flag)
        select /*+ parallel(nrd,5,1) */
         date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         0 as impressions_delivered, 0 as imps_888_11,
         0 as group_imps, 'N' as distribution_flag
        from   ben_yield_clk_no_imps nrd
        where  date_id >=
               to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
        and    date_id <=
               to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd'));
      commit;
      --main insert
      insert /*+ append */
      into ben_yield_master_stg1
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, imps_888_11, group_imps,
         distribution_flag)
        with src as
         (select /*+ parallel(nrd,5,1) */
           nrd.date_id, nrd.affiliate_id, nrd.ad_id,
           nrd.country, nrd.impression_type, nrd.ad_size_id,
           coalesce(nrd.ad_group_id, to_number(group_id)) as ad_group_id,
           sum(nrd.clicks) as clicks,
           sum(nrd.impressions_delivered) as impressions_delivered,
           sum(case
                 when nrd.ad_size_id = '888x11'
                 --and int_price_type = 'CPM'*/
                  then
                  nrd.impressions_delivered
                 else
                  0
               end) imps_888_11,
           case
             when nrd.ad_size_id <> '888x11' then
              sum(nrd.impressions_delivered)
             else
              0
           end group_imps
          from   ben_yield_network_rev nrd,
                 ods_metadata.adm_ads b
          /*,
          (select nae.ad_id, int_price_type
            from   oms_data.ad_average_price@nap avp,
                   ods_metadata.adm_ads nae
            where  avp.is_current = 1
            and    avp.view_date =
                   to_date(p_process_date, 'YYYYMMDD')
            and    avp.int_price_type = 'CPM'
            and    nae.ad_id = avp.ad_id
            and    nae.category = 'Advertiser'
            and    nae.ad_size_id = '888x11') c*/
          where  date_id >=
                 to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
          and    date_id <=
                 to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd'))
          and    category not in ('Ad Server', 'Internal')
          and    nrd.ad_id = b.ad_id
          --and    nrd.ad_id = c.ad_id(+)
          group  by nrd.affiliate_id, nrd.date_id, nrd.ad_id,
                    nrd.country, nrd.impression_type,
                    nrd.ad_size_id,
                    coalesce(nrd.ad_group_id, to_number(group_id))),
        src2 as
         (select distinct ad_group_id, 'x' as status
          from   src
          where  imps_888_11 > 0
          and    ad_group_id is not null)
        select date_id, affiliate_id, ad_id, country,
               impression_type, ad_size_id, s.ad_group_id,
               clicks, impressions_delivered, imps_888_11,
               group_imps,
               case
                 when coalesce(status, '-1') = 'x' then
                  'Y'
                 else
                  'N'
               end as distribution_flag
        from   src s, src2 s2
        where  s.ad_group_id = s2.ad_group_id(+);
      /*select s.*, 'N' as distribution_flag
      from   src s
      where  ad_group_id not in
             (select distinct ad_group_id
              from   src
              where  imps_888_11 > 0
              and    ad_group_id is not null)
      or     ad_group_id is null
      union all
      select s.*, 'Y' as distribution_flag
      from   src s
      where  ad_group_id in
             (select distinct ad_group_id
              from   src
              where  imps_888_11 > 0
              and    ad_group_id is not null);*/
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_master_stg1 estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----
    l_process_name := p_process_name || '-' ||
                      'ben_yield_master_stg2';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_yield_master_stg2';
      insert /*+ append */
      into ben_yield_master_stg2
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, imps_888_11, ad_id_888_11,
         distribution_flag)
        with raw_src as
         (select *
          from   ben_yield_master_stg1
          where  distribution_flag = 'Y'),
        src as
         (select affiliate_id, date_id, country,
                 impression_type,
                 sum(imps_888_11) imps_888_11,
                 sum(group_imps) group_imps, ad_group_id
          from   raw_src
          group  by affiliate_id, date_id, country,
                    impression_type, ad_group_id),
        src2 as
         (select a.affiliate_id, a.date_id, a.country,
                 a.impression_type, a.imps_888_11,
                 a.group_imps, a.ad_group_id, b.ad_id
          from   src a, raw_src b
          where  a.date_id = b.date_id
          and    a.affiliate_id = b.affiliate_id
          and    a.country = b.country
          and    a.impression_type = b.impression_type
          and    a.ad_group_id = b.ad_group_id
          and    b.ad_size_id = '888x11')
        select s.date_id, s.affiliate_id, s.ad_id, s.country,
               s.impression_type, s.ad_size_id,
               s.ad_group_id, sum(clicks) as clicks,
               sum(s.impressions_delivered) as impressions_delivered,
               coalesce(sum(s2.imps_888_11 *
                             impressions_delivered /
                             s2.group_imps), 0) as imps_888_11,
               s2.ad_id as ad_id_888_11, distribution_flag
        from   raw_src s, src2 s2
        where  s.date_id = s2.date_id(+)
        and    s.affiliate_id = s2.affiliate_id(+)
        and    s.country = s2.country(+)
        and    s.impression_type = s2.impression_type(+)
        and    s.ad_group_id = s2.ad_group_id(+)
        and    s.ad_size_id <> '888x11'
        group  by s.date_id, s.affiliate_id, s.ad_id,
                  s.country, s.impression_type, s.ad_size_id,
                  s.ad_group_id, s2.ad_id, distribution_flag;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_master_stg2 estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----
    l_process_name := p_process_name || '-' ||
                      'ben_yield_master_stg3';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_yield_master_stg3';
      insert /*+ append */
      into ben_yield_master_stg3
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, imps_888_11, ad_id_888_11,
         distribution_flag, ad_type, advertiser,
         affiliate_product_name, ad_category)
        with src as
         (select date_id, affiliate_id, ad_id, country,
                 impression_type, ad_size_id, ad_group_id,
                 clicks, impressions_delivered,
                 0 as imps_888_11, null as ad_id_888_11,
                 distribution_flag
          from   ben_yield_master_stg1
          where  distribution_flag = 'N'
          union all
          select date_id, affiliate_id, ad_id, country,
                 impression_type, ad_size_id, ad_group_id,
                 clicks, impressions_delivered, imps_888_11,
                 ad_id_888_11, distribution_flag
          from   ben_yield_master_stg2)
        /*,oms_ads as
        (select pliad.ad_id, pdmad.ad_product_id,
                pdmad.publisher_product_label as affiliate_product_name
         from   pdm_data.pdm_ad_line_item_ad_product@nap pliad,
                pdm_data.pdm_ad_product@nap pdmad
         where  pliad.ad_product_id = pdmad.ad_product_id)*/
        select /*+ parallel(nrdp,4,1) */
         date_id, affiliate_id, nrdp.ad_id, country,
         impression_type, nrdp.ad_size_id, ad_group_id,
         clicks, impressions_delivered, imps_888_11,
         ad_id_888_11, distribution_flag,
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
           when nae.category = 'Advertiser' then
            'Advertiser'
           when nae.category = 'Brand Research' then
            'Brand Research'
           when nae.category = 'Region Development' then
            'Region Development'
           when nae.category = 'Glam Advertising Partners' then
            'Glam Advertising Partners'
           when nae.category = 'Glam' then
            'Glam'
           when nae.category = 'Passthrough' then
            'Passthrough'
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
                coalesce(nae.sub_category, 'No Ad') =
                'No Ad' then
            'No Ad'
           else
            nae.category
         end as ad_category
        from   src nrdp, ods_metadata.adm_ads nae,
               ben_oms_pdm_ad_product omsp
        where  category not in ('Ad Server', 'Internal')
        and    nrdp.ad_id = omsp.ad_id(+)
        and    nrdp.ad_id = nae.ad_id(+);
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_master_stg3 estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ----
    l_process_name := p_process_name || '-' ||
                      'ben_yield_master_stg4';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table ben_yield_master_stg4';
      --no distribution impressions
      insert /*+ append */
      into ben_yield_master_stg4
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed,
         ad_type, advertiser, affiliate_product_name,
         ad_category, dummy_ad_size)
        select date_id, affiliate_id, ad_id, country,
               impression_type, ad_size_id, ad_group_id,
               clicks, impressions_delivered,
               impressions_delivered as impressions_distributed,
               ad_type, advertiser, affiliate_product_name,
               ad_category, ad_size_id as dummy_ad_size
        from   ben_yield_master_stg3
        where  distribution_flag = 'N';
      commit;
      --distribution impressions ad_groups
      insert /*+ append */
      into ben_yield_master_stg4
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed,
         ad_type, advertiser, affiliate_product_name,
         ad_category, dummy_ad_size)
        select date_id, affiliate_id, ad_id, country,
               impression_type, ad_size_id, ad_group_id,
               clicks, impressions_delivered,
               impressions_delivered as impressions_distributed,
               ad_type, advertiser, affiliate_product_name,
               ad_category, ad_size_id as dummy_ad_size
        from   ben_yield_master_stg3
        where  distribution_flag = 'Y';
      commit;
      --distribution impressions ad_groups but with 888x11 ad_id's
      insert /*+ append */
      into ben_yield_master_stg4
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed,
         ad_type, advertiser, affiliate_product_name,
         ad_category, dummy_ad_size)
        select date_id, affiliate_id, ad_id_888_11 as ad_id,
               country, impression_type, ad_size_id,
               ad_group_id, clicks,
               0 as impressions_delivered,
               imps_888_11 as impressions_distributed,
               ad_type, advertiser, affiliate_product_name,
               ad_category, '888x11' as dummy_ad_size
        from   ben_yield_master_stg3
        where  distribution_flag = 'Y'
        and    imps_888_11 > 0;
      commit;
      --inventory impressions as PDA for AD's running on API2.0
      insert /*+ append */
      into ben_yield_master_stg4
        (date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed,
         ad_type, advertiser, affiliate_product_name,
         ad_category, dummy_ad_size)
        select /*+ parallel(a,5,1) */
         date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id,
         0 as clicks,
         sum(inventory_impressions) as impressions_delivered,
         0 as impressions_distributed,
         case
           when impression_type = 'P' then
            'Premium Default Ads'
           when impression_type = 'M' then
            'GlamX Default Ads'
         end as ad_type, '' as advertiser,
         '' as affiliate_product_name,
         'Publisher Provided' as ad_category,
         ad_size_id as dummy_ad_size
        from   ben_yield_nrd a
        where  date_id >=
               to_number(to_char(trunc(to_date(p_process_date, 'yyyymmdd'), 'mm'), 'yyyymmdd'))
        and    date_id <=
               to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd'))
        and    inventory_impressions > 0
        and    order_id in (50004156, 50004251)
        group  by date_id, affiliate_id, ad_id, country,
                  impression_type, ad_size_id, ad_group_id;
      commit;
      execute immediate 'analyze table ben_yield_master_stg4 estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', 0);
    end if;
    l_process_name := p_process_name || '-' ||
                      'ben_yield_revenue';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_yield_revenue truncate partition P_' ||
                        substr(p_process_date, 1, 6);
      insert /*+ append */
      into ben_yield_revenue
        (month_id, date_id, affiliate_id, ad_id, country,
         impression_type, ad_size_id, ad_group_id, clicks,
         impressions_delivered, impressions_distributed,
         ad_type, advertiser, affiliate_product_name,
         ad_category, dummy_ad_size)
        select /*+ parallel(a,4) */
         substr(date_id, 1, 6), date_id, affiliate_id, ad_id,
         country, impression_type, ad_size_id, ad_group_id,
         sum(clicks), sum(impressions_delivered),
         sum(impressions_distributed), ad_type, advertiser,
         affiliate_product_name, ad_category, dummy_ad_size
        from   ben_yield_master_stg4 a
        group  by month_id, date_id, affiliate_id, ad_id,
                  country, impression_type, ad_size_id,
                  ad_group_id, ad_type, advertiser,
                  affiliate_product_name, ad_category,
                  dummy_ad_size;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_yield_revenue partition(P_' ||
                        substr(p_process_date, 1, 6) ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_ben_traffic_revenue
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'ben_traffic_revenue_daily';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'Alter table ben_traffic_revenue_daily truncate partition P_' ||
                        p_process_date;
      insert /*+ append */
      into ben_traffic_revenue_daily
        (date_id, affiliate_id, content_partner_id,
         impressions_delivered, clicks, country,
         impression_type, ad_category, ad_dimension, ad_id)
        select /*+ parallel(a,4,1) */
         date_id, affiliate_id, content_partner_id,
         sum(impressions_delivered) as impressions_delivered,
         sum(clicks) as clicks, country, impression_type,
         category, ad_size_id as ad_dimension, a.ad_id
        from   ben_network_revenue_daily a,
               ods_metadata.adm_ads b
        where  date_id = p_process_date
        and    coalesce(content_partner_id, -1) <> -1
        and    country = 'WW'
        and    impression_type in ('A', 'G', 'P')
        and    impressions_delivered > 0
        and    a.ad_id = b.ad_id
        and    b.category in
               ('Advertiser', 'Glam Advertising Partners')
        and    b.ad_size_id in
               ('160x600', '300x250', '300x600', '728x90', '800x150', '630x150', '270x150', '800x50', '160x160', '650x35', '984x258', '990x26', '728x91', '120x600')
        group  by date_id, affiliate_id, content_partner_id,
                  country, impression_type, category,
                  ad_size_id, a.ad_id;
      l_cnt := sql%rowcount;
      commit;
      execute immediate 'analyze table ben_traffic_revenue_daily partition(P_' ||
                        p_process_date ||
                        ') estimate statistics';
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure replicate_to_www
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    ---- yield_970x66_adserver@www
    l_process_name := p_process_name || '-' ||
                      'yield_970x66_adserver@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'yield_970x66_adserver_1', 'P_' ||
                                                              p_process_date);
      insert /*+ append */
      into insider_data.yield_970x66_adserver_1@etlwww
        (date_id, affiliate_id, ad_type, ad_size,
         impressions_delivered, clicks, preliminary_earnings,
         country, advertiser, affiliate_product_name,
         ad_category, ad_id)
        select date_id, affiliate_id, ad_type, ad_size,
               impressions_delivered, clicks,
               preliminary_earnings, country, advertiser,
               affiliate_product_name, ad_category, ad_id
        from   ben_yield_970x66_adserver_1
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'yield_970x66_adserver_1', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    ---- traffic_revenue_daily@www
    l_process_name := p_process_name || '-' ||
                      'traffic_revenue_daily@etlwww';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'traffic_revenue_daily', 'P_' ||
                                                              p_process_date);
      insert /*+ append */
      into insider_data.traffic_revenue_daily@etlwww
        (date_id, affiliate_id, content_partner_id,
         impressions_delivered, clicks, country,
         impression_type, ad_category, ad_dimension, ad_id)
        select date_id, affiliate_id, content_partner_id,
               impressions_delivered, clicks, country,
               impression_type, ad_category, ad_dimension,
               ad_id
        from   ben_traffic_revenue_daily
        where  date_id = p_process_date;
      l_cnt := sql%rowcount;
      commit;
      insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'traffic_revenue_daily', 'P_' ||
                                                             p_process_date);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- do not update the yield_revenue@www table during the reprocess, since AP is not runnig for reprocess
    -- But we need to add the kind of provision that it should be updated during the reprocess on last day of month
    if upper(p_process_name) = 'PROCESS' or
       p_process_date =
       to_number(to_char(last_day(to_date(p_process_date, 'yyyymmdd')), 'yyyymmdd')) then
      -- ben_yield_revenue
      l_process_name := p_process_name || '-' ||
                        'yield_revenue@etlwww';
      if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
        insider_data.pkg_generic_ddl.truncate_partition@etlwww('insider_data', 'yield_revenue', 'P_' ||
                                                                substr(p_process_date, 1, 6));
        insert /*+ append */
        into insider_data.yield_revenue@etlwww
          (month_id, date_id, affiliate_id, ad_id, country,
           impression_type, ad_size_id, ad_group_id, clicks,
           impressions_delivered, impressions_distributed,
           ad_type, advertiser, affiliate_product_name,
           ad_category, dummy_ad_size)
          select /*+ parallel(a,5,1) */
           month_id, date_id, affiliate_id, ad_id, country,
           impression_type, ad_size_id, ad_group_id, clicks,
           impressions_delivered, impressions_distributed,
           ad_type, advertiser, affiliate_product_name,
           ad_category, dummy_ad_size
          from   ben_yield_revenue a
          where  month_id = substr(p_process_date, 1, 6);
        l_cnt := sql%rowcount;
        commit;
        insider_data.pkg_generic_ddl.analyze_partition@etlwww('insider_data', 'yield_revenue', 'P_' ||
                                                               substr(p_process_date, 1, 6));
        pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
      end if;
      -- drop the old partitions from ben_yield_revenue@www table prior to 3 months
      l_process_name := p_process_name || '-' ||
                        'drop_part_yield_revenue@etlwww';
      if to_char(substr(p_process_date, 7, 8)) = '01' then
        if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
          pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
          insider_data.pkg_generic_ddl.drop_partition@etlwww('insider_data', 'yield_revenue', 'P_' ||
                                                              to_char(add_months(to_date(p_process_date, 'yyyymmdd'), -12), 'yyyymm'));
          pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
        end if;
      end if;
    end if;
  end;

end;
/
