create or replace package pkg_dimension_all as

  procedure run_dimension
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_channel_agg_ods
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_channel_agg_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_network_ad_ext_ods
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_network_ad_ext_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_dim_channel
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_dimension_ad_ads
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

  procedure load_dimension_ben
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  );

end pkg_dimension_all;
/
create or replace package body pkg_dimension_all as

  procedure run_dimension
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'run_dimension';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      --load_channel_agg_ods(p_run_id, p_process_date, l_process_name);
      --load_channel_agg_adq(p_run_id, p_process_date, l_process_name);
      --load_network_ad_ext_ods(p_run_id, p_process_date, l_process_name);
      --load_network_ad_ext_adq(p_run_id, p_process_date, l_process_name);
      load_dim_channel(p_run_id, p_process_date, l_process_name);
      load_dimension_ad_ads(p_run_id, p_process_date, l_process_name);
      load_dimension_ben(p_run_id, p_process_date, l_process_name);
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_channel_agg_ods
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'channel_aggregate_ods';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from ods_metadata.channel_aggregate;
      insert into ods_metadata.channel_aggregate
        (dart_site_id, channel_name, affiliate_id,
         affiliate_url, site_tags, status, network, country,
         currency, affiliate_type, list_tags, pepe_dart_tags,
         site_tags_all, title, list_id, parent_list_id,
         parent_collection_name, parent_name_full,
         collection_name, dart_site_name, group_name,
         group_name_null, group_aff_id, category_name_full,
         category_type_full, site_st_aty, site_pri_glam,
         site_pri_beauty, site_pri_celeb, site_pri_ent,
         site_pri_fashion, site_pri_lifestyle,
         site_pri_living, site_pri_lifetime, site_pri_shop,
         site_pri_teen, site_pri_com, site_pri_health,
         site_pri_quiz, site_pri_wed, site_pri_family,
         site_pri_lux, site_pri_aut, site_pri_tec,
         site_pri_category, site_pri_count,
         site_uber_category, site_glam_hp, site_brand_blast,
         ron, site_ec_glam, site_tar_beauty, site_tar_celeb,
         site_tar_ent, site_tar_fashion, site_tar_fitness,
         site_tar_gadgets_tech, site_tar_lifestyle,
         site_tar_living, site_tar_lifetime, site_tar_mom,
         site_tar_shop, site_tar_teen, site_tar_com,
         site_tar_health, site_tar_quiz, site_tar_wed,
         site_tar_family, site_tar_baby, site_tar_pregnancy,
         site_tar_parenting, site_pre_baut, site_pre_bent,
         site_pre_bls, site_pre_btec, site_pre_beauty,
         site_pre_celeb, site_pre_ent, site_pre_fashion,
         site_pre_fitness, site_pre_gadgets_tech,
         site_pre_lifestyle, site_pre_living,
         site_pre_lifetime, site_pre_mom, site_pre_shop,
         site_pre_teen, site_pre_com, site_pre_health,
         site_pre_quiz, site_pre_wed, site_pre_family,
         site_pre_baby, site_pre_pregnancy,
         site_pre_parenting, site_pre_luxury,
         site_pre_fajewel, site_pre_fineliving,
         site_pre_luxtravel, site_spe_org_new_age_mom,
         site_spe_african_american, site_spe_gay_lesbian,
         site_spe_luxury, site_spe_neiman_bergdorf,
         site_spe_armani, site_spe_women35plus,
         site_spe_urban, site_delivery_rank, site_p1,
         site_p2, site_p3, site_p4, site_p0, site_p_1234,
         site_tlc_glam, site_tlc_gnet, site_ha_glam,
         site_ha_gnet, site_google_adsense_ad,
         site_remnant_advertiser, site_glam_blog,
         site_glam_oo, site_non_user_gen,
         site_sitetag_iframe, site_sitetag_js,
         site_rm_expandable, site_rm_overlay, site_rm_atlas,
         site_rm_pointroll, site_rm_eyewonder,
         site_rm_eyeblaster, site_rm_dart_motif,
         site_rb_728x300, site_rb_728x300_af,
         site_rb_728x160, site_rb_728x160_af,
         site_rb_160x300, site_rb_160x300_af, site_rb_all_3,
         site_rb_all_3_af, site_country_uk, site_ex_explicit,
         site_ex_borderline, site_ex_alcohol,
         site_ex_non_brand, site_ex_dating,
         site_ex_horoscopes, site_ex_medical, site_ex_risque,
         site_ex_surveys, site_ex_weight_loss, site_ex_png,
         site_ex_b5, site_ex_ha_glam, site_ex_ha_glamnet,
         site_ex_glam_tlc, site_ex_glamnet_tlc, site_ex_nptr,
         pp_st_ghb, pp_cpc_st, parent_affiliate_id,
         is_anchor, is_only_glamex, is_atac, atp_rank, pec,
         glambrash_site, brash_site, gap_amount, iash_value,
         is_blog, creation_date, withdrawn_date,
         is_outofnetwork_buy, iash_signoff_status,
         iash_is_targetted, contract_start_date,
         contract_end_date, rss_feed_url, editorial_rank,
         social_rank)
        select dart_site_id, channel_name, affiliate_id,
               affiliate_url, site_tags, status, network,
               country, currency, affiliate_type, list_tags,
               pepe_dart_tags, site_tags_all, title, list_id,
               parent_list_id, parent_collection_name,
               parent_name_full, collection_name,
               dart_site_name, group_name, group_name_null,
               group_aff_id, category_name_full,
               category_type_full, site_st_aty,
               site_pri_glam, site_pri_beauty,
               site_pri_celeb, site_pri_ent,
               site_pri_fashion, site_pri_lifestyle,
               site_pri_living, site_pri_lifetime,
               site_pri_shop, site_pri_teen, site_pri_com,
               site_pri_health, site_pri_quiz, site_pri_wed,
               site_pri_family, site_pri_lux, site_pri_aut,
               site_pri_tec, site_pri_category,
               site_pri_count, site_uber_category,
               site_glam_hp, site_brand_blast, ron,
               site_ec_glam, site_tar_beauty, site_tar_celeb,
               site_tar_ent, site_tar_fashion,
               site_tar_fitness, site_tar_gadgets_tech,
               site_tar_lifestyle, site_tar_living,
               site_tar_lifetime, site_tar_mom,
               site_tar_shop, site_tar_teen, site_tar_com,
               site_tar_health, site_tar_quiz, site_tar_wed,
               site_tar_family, site_tar_baby,
               site_tar_pregnancy, site_tar_parenting,
               site_pre_baut, site_pre_bent, site_pre_bls,
               site_pre_btec, site_pre_beauty,
               site_pre_celeb, site_pre_ent,
               site_pre_fashion, site_pre_fitness,
               site_pre_gadgets_tech, site_pre_lifestyle,
               site_pre_living, site_pre_lifetime,
               site_pre_mom, site_pre_shop, site_pre_teen,
               site_pre_com, site_pre_health, site_pre_quiz,
               site_pre_wed, site_pre_family, site_pre_baby,
               site_pre_pregnancy, site_pre_parenting,
               site_pre_luxury, site_pre_fajewel,
               site_pre_fineliving, site_pre_luxtravel,
               site_spe_org_new_age_mom,
               site_spe_african_american,
               site_spe_gay_lesbian, site_spe_luxury,
               site_spe_neiman_bergdorf, site_spe_armani,
               site_spe_women35plus, site_spe_urban,
               site_delivery_rank, site_p1, site_p2, site_p3,
               site_p4, site_p0, site_p_1234, site_tlc_glam,
               site_tlc_gnet, site_ha_glam, site_ha_gnet,
               site_google_adsense_ad,
               site_remnant_advertiser, site_glam_blog,
               site_glam_oo, site_non_user_gen,
               site_sitetag_iframe, site_sitetag_js,
               site_rm_expandable, site_rm_overlay,
               site_rm_atlas, site_rm_pointroll,
               site_rm_eyewonder, site_rm_eyeblaster,
               site_rm_dart_motif, site_rb_728x300,
               site_rb_728x300_af, site_rb_728x160,
               site_rb_728x160_af, site_rb_160x300,
               site_rb_160x300_af, site_rb_all_3,
               site_rb_all_3_af, site_country_uk,
               site_ex_explicit, site_ex_borderline,
               site_ex_alcohol, site_ex_non_brand,
               site_ex_dating, site_ex_horoscopes,
               site_ex_medical, site_ex_risque,
               site_ex_surveys, site_ex_weight_loss,
               site_ex_png, site_ex_b5, site_ex_ha_glam,
               site_ex_ha_glamnet, site_ex_glam_tlc,
               site_ex_glamnet_tlc, site_ex_nptr, pp_st_ghb,
               pp_cpc_st, parent_affiliate_id, is_anchor,
               is_only_glamex, is_atac, atp_rank, pec,
               glambrash_site, brash_site, gap_amount,
               iash_value, is_blog, creation_date,
               withdrawn_date, is_outofnetwork_buy,
               iash_signoff_status, iash_is_targetted,
               contract_start_date, contract_end_date,
               rss_feed_url, editorial_rank, social_rank
        from   ap_data.channel_aggregate_view@etlnap;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_channel_agg_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    l_process_name := p_process_name || '-' ||
                      'channel_aggregate@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from channel_aggregate@etladq;
      insert into channel_aggregate@etladq
        (dart_site_id, channel_name, affiliate_id,
         affiliate_url, site_tags, status, network, country,
         currency, affiliate_type, list_tags, pepe_dart_tags,
         site_tags_all, title, list_id, parent_list_id,
         parent_collection_name, parent_name_full,
         collection_name, dart_site_name, group_name,
         group_name_null, group_aff_id, category_name_full,
         category_type_full, site_st_aty, site_pri_glam,
         site_pri_beauty, site_pri_celeb, site_pri_ent,
         site_pri_fashion, site_pri_lifestyle,
         site_pri_living, site_pri_lifetime, site_pri_shop,
         site_pri_teen, site_pri_com, site_pri_health,
         site_pri_quiz, site_pri_wed, site_pri_family,
         site_pri_lux, site_pri_aut, site_pri_tec,
         site_pri_category, site_pri_count,
         site_uber_category, site_glam_hp, site_brand_blast,
         ron, site_ec_glam, site_tar_beauty, site_tar_celeb,
         site_tar_ent, site_tar_fashion, site_tar_fitness,
         site_tar_gadgets_tech, site_tar_lifestyle,
         site_tar_living, site_tar_lifetime, site_tar_mom,
         site_tar_shop, site_tar_teen, site_tar_com,
         site_tar_health, site_tar_quiz, site_tar_wed,
         site_tar_family, site_tar_baby, site_tar_pregnancy,
         site_tar_parenting, site_pre_baut, site_pre_bent,
         site_pre_bls, site_pre_btec, site_pre_beauty,
         site_pre_celeb, site_pre_ent, site_pre_fashion,
         site_pre_fitness, site_pre_gadgets_tech,
         site_pre_lifestyle, site_pre_living,
         site_pre_lifetime, site_pre_mom, site_pre_shop,
         site_pre_teen, site_pre_com, site_pre_health,
         site_pre_quiz, site_pre_wed, site_pre_family,
         site_pre_baby, site_pre_pregnancy,
         site_pre_parenting, site_pre_luxury,
         site_pre_fajewel, site_pre_fineliving,
         site_pre_luxtravel, site_spe_org_new_age_mom,
         site_spe_african_american, site_spe_gay_lesbian,
         site_spe_luxury, site_spe_neiman_bergdorf,
         site_spe_armani, site_spe_women35plus,
         site_spe_urban, site_delivery_rank, site_p1,
         site_p2, site_p3, site_p4, site_p0, site_p_1234,
         site_tlc_glam, site_tlc_gnet, site_ha_glam,
         site_ha_gnet, site_google_adsense_ad,
         site_remnant_advertiser, site_glam_blog,
         site_glam_oo, site_non_user_gen,
         site_sitetag_iframe, site_sitetag_js,
         site_rm_expandable, site_rm_overlay, site_rm_atlas,
         site_rm_pointroll, site_rm_eyewonder,
         site_rm_eyeblaster, site_rm_dart_motif,
         site_rb_728x300, site_rb_728x300_af,
         site_rb_728x160, site_rb_728x160_af,
         site_rb_160x300, site_rb_160x300_af, site_rb_all_3,
         site_rb_all_3_af, site_country_uk, site_ex_explicit,
         site_ex_borderline, site_ex_alcohol,
         site_ex_non_brand, site_ex_dating,
         site_ex_horoscopes, site_ex_medical, site_ex_risque,
         site_ex_surveys, site_ex_weight_loss, site_ex_png,
         site_ex_b5, site_ex_ha_glam, site_ex_ha_glamnet,
         site_ex_glam_tlc, site_ex_glamnet_tlc, site_ex_nptr,
         pp_st_ghb, pp_cpc_st, parent_affiliate_id,
         is_anchor, is_only_glamex, is_atac, atp_rank, pec,
         glambrash_site, brash_site, gap_amount, iash_value,
         is_blog, creation_date, withdrawn_date,
         is_outofnetwork_buy, iash_signoff_status,
         iash_is_targetted, contract_start_date,
         contract_end_date, rss_feed_url, editorial_rank,
         social_rank)
        select dart_site_id, channel_name, affiliate_id,
               affiliate_url, site_tags, status, network,
               country, currency, affiliate_type, list_tags,
               pepe_dart_tags, site_tags_all, title, list_id,
               parent_list_id, parent_collection_name,
               parent_name_full, collection_name,
               dart_site_name, group_name, group_name_null,
               group_aff_id, category_name_full,
               category_type_full, site_st_aty,
               site_pri_glam, site_pri_beauty,
               site_pri_celeb, site_pri_ent,
               site_pri_fashion, site_pri_lifestyle,
               site_pri_living, site_pri_lifetime,
               site_pri_shop, site_pri_teen, site_pri_com,
               site_pri_health, site_pri_quiz, site_pri_wed,
               site_pri_family, site_pri_lux, site_pri_aut,
               site_pri_tec, site_pri_category,
               site_pri_count, site_uber_category,
               site_glam_hp, site_brand_blast, ron,
               site_ec_glam, site_tar_beauty, site_tar_celeb,
               site_tar_ent, site_tar_fashion,
               site_tar_fitness, site_tar_gadgets_tech,
               site_tar_lifestyle, site_tar_living,
               site_tar_lifetime, site_tar_mom,
               site_tar_shop, site_tar_teen, site_tar_com,
               site_tar_health, site_tar_quiz, site_tar_wed,
               site_tar_family, site_tar_baby,
               site_tar_pregnancy, site_tar_parenting,
               site_pre_baut, site_pre_bent, site_pre_bls,
               site_pre_btec, site_pre_beauty,
               site_pre_celeb, site_pre_ent,
               site_pre_fashion, site_pre_fitness,
               site_pre_gadgets_tech, site_pre_lifestyle,
               site_pre_living, site_pre_lifetime,
               site_pre_mom, site_pre_shop, site_pre_teen,
               site_pre_com, site_pre_health, site_pre_quiz,
               site_pre_wed, site_pre_family, site_pre_baby,
               site_pre_pregnancy, site_pre_parenting,
               site_pre_luxury, site_pre_fajewel,
               site_pre_fineliving, site_pre_luxtravel,
               site_spe_org_new_age_mom,
               site_spe_african_american,
               site_spe_gay_lesbian, site_spe_luxury,
               site_spe_neiman_bergdorf, site_spe_armani,
               site_spe_women35plus, site_spe_urban,
               site_delivery_rank, site_p1, site_p2, site_p3,
               site_p4, site_p0, site_p_1234, site_tlc_glam,
               site_tlc_gnet, site_ha_glam, site_ha_gnet,
               site_google_adsense_ad,
               site_remnant_advertiser, site_glam_blog,
               site_glam_oo, site_non_user_gen,
               site_sitetag_iframe, site_sitetag_js,
               site_rm_expandable, site_rm_overlay,
               site_rm_atlas, site_rm_pointroll,
               site_rm_eyewonder, site_rm_eyeblaster,
               site_rm_dart_motif, site_rb_728x300,
               site_rb_728x300_af, site_rb_728x160,
               site_rb_728x160_af, site_rb_160x300,
               site_rb_160x300_af, site_rb_all_3,
               site_rb_all_3_af, site_country_uk,
               site_ex_explicit, site_ex_borderline,
               site_ex_alcohol, site_ex_non_brand,
               site_ex_dating, site_ex_horoscopes,
               site_ex_medical, site_ex_risque,
               site_ex_surveys, site_ex_weight_loss,
               site_ex_png, site_ex_b5, site_ex_ha_glam,
               site_ex_ha_glamnet, site_ex_glam_tlc,
               site_ex_glamnet_tlc, site_ex_nptr, pp_st_ghb,
               pp_cpc_st, parent_affiliate_id, is_anchor,
               is_only_glamex, is_atac, atp_rank, pec,
               glambrash_site, brash_site, gap_amount,
               iash_value, is_blog, creation_date,
               withdrawn_date, is_outofnetwork_buy,
               iash_signoff_status, iash_is_targetted,
               contract_start_date, contract_end_date,
               rss_feed_url, editorial_rank, social_rank
        from   ods_metadata.channel_aggregate;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    /*l_process_name := p_process_name || '-' ||
                      'campaign.adq_aff_list_category@adq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from campaign.adq_aff_list_category@adq;
      insert into campaign.adq_aff_list_category@adq
        (adq_list_name, list_category)
        select ld.adq_name, adq_grp.group_name
        from   list_data.list@nap ld,
               list_data.adq_groups@nap adq_grp
        where  adq_grp.group_id = ld.adq_group;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;*/
  end;

  procedure load_network_ad_ext_ods
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- network_ad_extended
    l_process_name := p_process_name || '-' ||
                      'network_ad_extended@etlods';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from ods_metadata.network_ad_extended;
      insert into ods_metadata.network_ad_extended
        (source_id, ad_id, ad_name, ad_dimension,
         advertiser_id, advertiser_name,
         impressions_delivered, clicks, ad_rate, order_id,
         ad_type, ad_category, ad_sub_category,
         ad_category_is_default, ad_currency, ad_country,
         ad_subsidiary, keyvalues, ad_industry,
         ad_industry_abv, ad_industry_is_lookup,
         ad_dimension_id, tacoda, coll_media, primetime_on,
         primetime_off, ad_glam_hp, ad_brand_blast, ron,
         ad_ec_glam, ad_spe_org_new_age_mom,
         ad_spe_african_american, ad_spe_gay_lesbian,
         ad_spe_luxury, ad_spe_neiman_bergdorf,
         ad_spe_armani, ad_spe_women35plus, ad_delivery_rank,
         ad_p1, ad_p2, ad_p3, ad_p4, ad_p0, ad_p_1234,
         ad_tlc_glam, ad_tlc_gnet, ad_ha_glam, ad_ha_gnet,
         ad_google_adsense_ad, ad_remnant_advertiser,
         ad_glam_blog, ad_glam_oo, ad_non_user_gen,
         ad_sitetag_iframe, ad_sitetag_js, ad_rm_expandable,
         ad_rm_overlay, ad_rm_atlas, ad_rm_pointroll,
         ad_rm_eyewonder, ad_rm_eyeblaster, ad_rm_dart_motif,
         ad_rb_728x300, ad_rb_728x300_af, ad_rb_728x160,
         ad_rb_728x160_af, ad_rb_160x300, ad_rb_160x300_af,
         ad_rb_all_3, ad_rb_all_3_af, ad_country_uk,
         ad_ex_explicit, ad_ex_borderline, ad_ex_alcohol,
         ad_ex_non_brand, ad_ex_dating, ad_ex_horoscopes,
         ad_ex_medical, ad_ex_risque, ad_ex_surveys,
         ad_ex_weight_loss, ad_ex_png, ad_ex_b5,
         ad_ex_ha_glam, ad_ex_ha_glamnet, ad_ex_glam_tlc,
         pp_st_ghb, is_active, start_date, end_date,
         order_start_date, order_end_date, order_name,
         order_booked_impressions, ad_value, offer_industry,
         offer_industry_abv, ctr_optimization_enabled)
        select source_id, ad_id, ad_name, ad_dimension,
               advertiser_id, advertiser_name,
               impressions_delivered, clicks, ad_rate,
               order_id, ad_type, ad_category,
               ad_sub_category, ad_category_is_default,
               ad_currency, ad_country, ad_subsidiary,
               keyvalues, ad_industry, ad_industry_abv,
               ad_industry_is_lookup, ad_dimension_id,
               tacoda, coll_media, primetime_on,
               primetime_off, ad_glam_hp, ad_brand_blast,
               ron, ad_ec_glam, ad_spe_org_new_age_mom,
               ad_spe_african_american, ad_spe_gay_lesbian,
               ad_spe_luxury, ad_spe_neiman_bergdorf,
               ad_spe_armani, ad_spe_women35plus,
               ad_delivery_rank, ad_p1, ad_p2, ad_p3, ad_p4,
               ad_p0, ad_p_1234, ad_tlc_glam, ad_tlc_gnet,
               ad_ha_glam, ad_ha_gnet, ad_google_adsense_ad,
               ad_remnant_advertiser, ad_glam_blog,
               ad_glam_oo, ad_non_user_gen,
               ad_sitetag_iframe, ad_sitetag_js,
               ad_rm_expandable, ad_rm_overlay, ad_rm_atlas,
               ad_rm_pointroll, ad_rm_eyewonder,
               ad_rm_eyeblaster, ad_rm_dart_motif,
               ad_rb_728x300, ad_rb_728x300_af,
               ad_rb_728x160, ad_rb_728x160_af,
               ad_rb_160x300, ad_rb_160x300_af, ad_rb_all_3,
               ad_rb_all_3_af, ad_country_uk, ad_ex_explicit,
               ad_ex_borderline, ad_ex_alcohol,
               ad_ex_non_brand, ad_ex_dating,
               ad_ex_horoscopes, ad_ex_medical, ad_ex_risque,
               ad_ex_surveys, ad_ex_weight_loss, ad_ex_png,
               ad_ex_b5, ad_ex_ha_glam, ad_ex_ha_glamnet,
               ad_ex_glam_tlc, pp_st_ghb, is_active,
               start_date, end_date, order_start_date,
               order_end_date, order_name,
               order_booked_impressions, ad_value,
               offer_industry, offer_industry_abv,
               ctr_optimization_enabled
        from   bodata.view_network_ad_combined@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_network_ad_ext_adq
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- network_ad_extended
    l_process_name := p_process_name || '-' ||
                      'network_ad_extended@etladq';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      delete from network_ad_extended@etladq;
      insert into network_ad_extended@etladq
        (source_id, ad_id, ad_name, ad_dimension,
         advertiser_id, advertiser_name,
         impressions_delivered, clicks, ad_rate, order_id,
         ad_type, ad_category, ad_sub_category,
         ad_category_is_default, ad_currency, ad_country,
         ad_subsidiary, keyvalues, ad_industry,
         ad_industry_abv, ad_industry_is_lookup,
         ad_dimension_id, tacoda, coll_media, primetime_on,
         primetime_off, ad_glam_hp, ad_brand_blast, ron,
         ad_ec_glam, ad_spe_org_new_age_mom,
         ad_spe_african_american, ad_spe_gay_lesbian,
         ad_spe_luxury, ad_spe_neiman_bergdorf,
         ad_spe_armani, ad_spe_women35plus, ad_delivery_rank,
         ad_p1, ad_p2, ad_p3, ad_p4, ad_p0, ad_p_1234,
         ad_tlc_glam, ad_tlc_gnet, ad_ha_glam, ad_ha_gnet,
         ad_google_adsense_ad, ad_remnant_advertiser,
         ad_glam_blog, ad_glam_oo, ad_non_user_gen,
         ad_sitetag_iframe, ad_sitetag_js, ad_rm_expandable,
         ad_rm_overlay, ad_rm_atlas, ad_rm_pointroll,
         ad_rm_eyewonder, ad_rm_eyeblaster, ad_rm_dart_motif,
         ad_rb_728x300, ad_rb_728x300_af, ad_rb_728x160,
         ad_rb_728x160_af, ad_rb_160x300, ad_rb_160x300_af,
         ad_rb_all_3, ad_rb_all_3_af, ad_country_uk,
         ad_ex_explicit, ad_ex_borderline, ad_ex_alcohol,
         ad_ex_non_brand, ad_ex_dating, ad_ex_horoscopes,
         ad_ex_medical, ad_ex_risque, ad_ex_surveys,
         ad_ex_weight_loss, ad_ex_png, ad_ex_b5,
         ad_ex_ha_glam, ad_ex_ha_glamnet, ad_ex_glam_tlc,
         pp_st_ghb, is_active, start_date, end_date,
         order_start_date, order_end_date, order_name,
         order_booked_impressions, ad_value, offer_industry,
         offer_industry_abv, ctr_optimization_enabled)
        select source_id, ad_id, ad_name, ad_dimension,
               advertiser_id, advertiser_name,
               impressions_delivered, clicks, ad_rate,
               order_id, ad_type, ad_category,
               ad_sub_category, ad_category_is_default,
               ad_currency, ad_country, ad_subsidiary,
               keyvalues, ad_industry, ad_industry_abv,
               ad_industry_is_lookup, ad_dimension_id,
               tacoda, coll_media, primetime_on,
               primetime_off, ad_glam_hp, ad_brand_blast,
               ron, ad_ec_glam, ad_spe_org_new_age_mom,
               ad_spe_african_american, ad_spe_gay_lesbian,
               ad_spe_luxury, ad_spe_neiman_bergdorf,
               ad_spe_armani, ad_spe_women35plus,
               ad_delivery_rank, ad_p1, ad_p2, ad_p3, ad_p4,
               ad_p0, ad_p_1234, ad_tlc_glam, ad_tlc_gnet,
               ad_ha_glam, ad_ha_gnet, ad_google_adsense_ad,
               ad_remnant_advertiser, ad_glam_blog,
               ad_glam_oo, ad_non_user_gen,
               ad_sitetag_iframe, ad_sitetag_js,
               ad_rm_expandable, ad_rm_overlay, ad_rm_atlas,
               ad_rm_pointroll, ad_rm_eyewonder,
               ad_rm_eyeblaster, ad_rm_dart_motif,
               ad_rb_728x300, ad_rb_728x300_af,
               ad_rb_728x160, ad_rb_728x160_af,
               ad_rb_160x300, ad_rb_160x300_af, ad_rb_all_3,
               ad_rb_all_3_af, ad_country_uk, ad_ex_explicit,
               ad_ex_borderline, ad_ex_alcohol,
               ad_ex_non_brand, ad_ex_dating,
               ad_ex_horoscopes, ad_ex_medical, ad_ex_risque,
               ad_ex_surveys, ad_ex_weight_loss, ad_ex_png,
               ad_ex_b5, ad_ex_ha_glam, ad_ex_ha_glamnet,
               ad_ex_glam_tlc, pp_st_ghb, is_active,
               start_date, end_date, order_start_date,
               order_end_date, order_name,
               order_booked_impressions, ad_value,
               offer_industry, offer_industry_abv,
               ctr_optimization_enabled
        from   ods_metadata.network_ad_extended;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_dim_channel
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- dim_channel_adq
    l_process_name := p_process_name || '-' ||
                      'dim_channel_adq@etladq';
  end;

  procedure load_dimension_ad_ads
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- dim_campaign
    l_process_name := p_process_name || '-' ||
                      'dim_campaign';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_campaign';
      insert into dim_campaign
        (advertiser_id, advertiser_name, order_id,
         order_name, order_start_date, order_end_date, ad_id,
         ad_name, ad_start_date, ad_end_date, created_by,
         created_date, updated_by, updated_date)
        select distinct advertiser_id, advertiser_name,
                        order_id, order_name,
                        order_start_date, order_end_date,
                        ad_id, name, start_date, end_date,
                        'GLAM', sysdate, 'GLAM', sysdate
        from   ods_metadata.adm_ads
        group  by advertiser_id, advertiser_name, order_id,
                  order_name, order_start_date,
                  order_end_date, ad_id, name, start_date,
                  end_date;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_advertiser
    l_process_name := p_process_name || '-' ||
                      'dim_advertiser';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_advertiser';
      insert into dim_advertiser
        (advertiser_id, advertiser_name, created_by,
         created_date, updated_by, updated_date)
        select distinct advertiser_id, advertiser_name,
                        'GLAM', sysdate, 'GLAM', sysdate
        from   ods_metadata.adm_ads;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_order
    l_process_name := p_process_name || '-' || 'dim_order';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_order';
      insert into dim_order
        (order_id, order_name, order_start_date,
         order_end_date, created_by, created_date,
         updated_by, updated_date)
        select distinct order_id, order_name,
                        order_start_date, order_end_date,
                        'GLAM', sysdate, 'GLAM', sysdate
        from   ods_metadata.adm_ads;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_ad_new
    l_process_name := p_process_name || '-' || 'dim_ad_new';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_ad_new';
      insert into dim_ad_new
        (ad_id, name, isapproved, ad_size_id, priority,
         deliveredimpressions, deliveredclicks, startdate,
         enddate, keyvalues, states, quantity, rate, value,
         adexclusions, pricingtype, alternate_id,
         delivery_scheduling, ad_type, comments,
         is_include_site_targeting, last_updated,
         same_advertiser_exception, status, tile_categories,
         category, sub_category, is_include_countries)
        select distinct ad_id, name, is_approved, ad_size_id,
                        priority,
                        null as deliveredimpressions,
                        null as deliveredclicks, start_date,
                        end_date, targeting_key_value,
                        state_region,
                        booked_impressions as quantity, rate,
                        value, adexclusions, pricing_type,
                        alternate_id, delivery_scheduling,
                        ad_type, null as comments,
                        is_inc_site_zone, updated_on,
                        null as same_advertiser_exception,
                        null as status, tile_category,
                        category, sub_category,
                        is_inc_country
        from   ods_metadata.adm_ads;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_adm_ads
    l_process_name := p_process_name || '-' ||
                      'dim_adm_ads';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_adm_ads';
      insert into dim_adm_ads
        (active, adexclusions, advertiser_active,
         advertiser_gad_country, advertiser_id,
         advertiser_info_value, advertiser_name,
         advertiser_updated_on, ad_id, ad_size_id, ad_type,
         agency, alternate_id, area_code, bandwidth,
         billing_source, booked_impressions,
         campaign_manager, category, cities, contains_survey,
         contain_floating_ad, countries, creative_count,
         dart_ad_id, dart_advertiser_id, dart_order_id,
         delivery_scheduling, delivery_type, dma, end_date,
         frequency, frequency_duration,
         frequency_duration_type, info_value,
         isp_network_code, is_approved, is_inc_area_code,
         is_inc_bandwidth, is_inc_country, is_inc_dma,
         is_inc_isp_network_code, is_inc_operating_system,
         is_inc_site, is_inc_site_zone, is_inc_state_region,
         is_inc_web_browser, is_inc_zip, keyword,
         launch_timezone, media_type, name,
         oms_advertiser_id, oms_order_id, operating_system,
         order_active, order_booked_impressions,
         order_end_date, order_id, order_name,
         order_pricing_type, order_rate, order_start_date,
         order_value, over_deliver, parent_advertiser,
         pricing_source, pricing_type, priority,
         process_updated_on, proposal_value, rate, rate_type,
         rep_firm, sales_person, sales_planner,
         same_adv_exception, second_campaign_manager, site,
         site_zone, source_id, start_date, state_region,
         sub_category, table_source_type,
         targeting_key_value, tile_category,
         time_delivery_type, updated_on, value, web_browser,
         weight, zip, delivery_progress, override_area_code,
         override_bandwidth, override_country,
         override_day_parting, override_dma,
         override_network_code, override_operating_system,
         override_site, override_site_zone,
         override_state_region, override_targeting_key_value,
         override_web_browser, override_zip,
         preset_key_value_exp)
        select active, adexclusions, advertiser_active,
               advertiser_gad_country, advertiser_id,
               advertiser_info_value, advertiser_name,
               advertiser_updated_on, ad_id, ad_size_id,
               ad_type, agency, alternate_id, area_code,
               bandwidth, billing_source, booked_impressions,
               campaign_manager, category, cities,
               contains_survey, contain_floating_ad,
               countries, creative_count, dart_ad_id,
               dart_advertiser_id, dart_order_id,
               delivery_scheduling, delivery_type, dma,
               end_date, frequency, frequency_duration,
               frequency_duration_type, info_value,
               isp_network_code, is_approved,
               is_inc_area_code, is_inc_bandwidth,
               is_inc_country, is_inc_dma,
               is_inc_isp_network_code,
               is_inc_operating_system, is_inc_site,
               is_inc_site_zone, is_inc_state_region,
               is_inc_web_browser, is_inc_zip, keyword,
               launch_timezone, media_type, name,
               oms_advertiser_id, oms_order_id,
               operating_system, order_active,
               order_booked_impressions, order_end_date,
               order_id, order_name, order_pricing_type,
               order_rate, order_start_date, order_value,
               over_deliver, parent_advertiser,
               pricing_source, pricing_type, priority,
               process_updated_on, proposal_value, rate,
               rate_type, rep_firm, sales_person,
               sales_planner, same_adv_exception,
               second_campaign_manager, site, site_zone,
               source_id, start_date, state_region,
               sub_category, table_source_type,
               targeting_key_value, tile_category,
               time_delivery_type, updated_on, value,
               web_browser, weight, zip, delivery_progress,
               override_area_code, override_bandwidth,
               override_country, override_day_parting,
               override_dma, override_network_code,
               override_operating_system, override_site,
               override_site_zone, override_state_region,
               override_targeting_key_value,
               override_web_browser, override_zip,
               preset_key_value_exp
        from   ods_metadata.adm_ads;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

  procedure load_dimension_ben
  (
    p_run_id       number,
    p_process_date number,
    p_process_name varchar2
  ) is
    l_process_name varchar2(100);
    l_cnt          number := 0;
  begin
    -- dim_states
    l_process_name := p_process_name || '-' || 'dim_states';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_states';
      insert into dim_states
        (state_province, state_province_name)
        select state_province, state_province_name
        from   dart_data.dart_states@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_cities
    l_process_name := p_process_name || '-' || 'dim_cities';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_cities';
      insert into dim_cities
        (city_id, city)
        select city_id, city
        from   dart_data.dart_cities@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_connection
    l_process_name := p_process_name || '-' ||
                      'dim_connection';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_connection';
      insert into dim_connection
        (connection_type_id, connection_type, comments)
        select connection_type_id, connection_type, comments
        from   dart_data.dart_connection_types@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_operatingsystems
    l_process_name := p_process_name || '-' ||
                      'dim_operatingsystems';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_operatingsystems';
      insert into dim_operatingsystems
        (os_id, os)
        select os_id, os
        from   dart_data.dart_operating_systems@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_zone
    l_process_name := p_process_name || '-' || 'dim_zone';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_zone';
      insert into dim_zone
        (zone_id, zone_keyname, is_explicitly_targeted,
         site_id, site, marketplace_zone, callback_zone,
         cb_advertiser_id, cb_advertiser_name, atako_zone,
         ga_zone)
        select zone_id, zone_keyname, is_explicitly_targeted,
               site_id, site, marketplace_zone,
               callback_zone, cb_advertiser_id,
               cb_advertiser_name, atako_zone, ga_zone
        from   dart_data.dart_zone@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_browsers
    l_process_name := p_process_name || '-' ||
                      'dim_browsers';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_browsers';
      insert into dim_browsers
        (browser_id, browser)
        select browser_id, browser
        from   dart_data.dart_browsers@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_countries
    l_process_name := p_process_name || '-' ||
                      'dim_countries';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_countries';
      insert into dim_countries
        (country_id, country, country_abbrev,
         country_currency, adaptive_code, is_targeted)
        select country_id, country, country_abbrev,
               country_currency, adaptive_code, is_targeted
        from   dart_data.dart_countries@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
    -- dim_creative
    l_process_name := p_process_name || '-' ||
                      'dim_creative';
    if pkg_log_process.is_not_complete(p_run_id, p_process_date, l_process_name) then
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'I');
      execute immediate 'truncate table dim_creative';
      insert into dim_creative
        (advertiser_id, creative_id, ui_creative_id,
         creative, creative_status, creative_type,
         creative_size_id, created_by, created_date)
        select advertiser_id, creative_id, ui_creative_id,
               creative, creative_status, creative_type,
               creative_size_id, 'GLAM', sysdate
        from   dart_data.dart_creative@etlben;
      l_cnt := sql%rowcount;
      commit;
      pkg_log_process.log_process(p_run_id, p_process_date, l_process_name, 'UC', l_cnt);
    end if;
  end;

end pkg_dimension_all;
/
