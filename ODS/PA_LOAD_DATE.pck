CREATE OR REPLACE PACKAGE PA_LOAD_DATE AS
/******************************************************************************
   NAME:       PA_LOAD_DATE
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        8/25/2008   Archana Mehta    1. Created this package.
******************************************************************************/

procedure sp_load_dim_time(
    i_num_step_run_id         in number,
    i_dat_step_start_date     in date,
    i_dat_step_end_date       in date,
    o_num_read_record_count   out number,
    o_num_write_record_count  out number,
    o_num_reject_record_count out number
);

procedure sp_load_dim_time(
    i_start_date date,
    i_end_date date
);

procedure sp_drop_all_data;


END PA_LOAD_DATE; 
/
CREATE OR REPLACE PACKAGE BODY PA_LOAD_DATE AS
/******************************************************************************
   NAME:       PA_LOAD_DATE
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        8/25/2008             1. Created this package body.
******************************************************************************/


procedure sp_load_dim_year(
    i_start_date date,
    i_end_date date
);

procedure sp_load_dim_quarter(
    i_start_date date,
    i_end_date date
);

procedure sp_load_dim_month(
    i_start_date date,
    i_end_date date
);

procedure sp_load_dim_week(
    i_start_date date,
    i_end_date date
);

procedure sp_load_dim_date(
    i_start_date date,
    i_end_date date
);


/**********************************************
*
* Implementations.
*
***********************************************/

procedure sp_load_dim_time(
    i_num_step_run_id         in number,
    i_dat_step_start_date     in date,
    i_dat_step_end_date       in date,
    o_num_read_record_count   out number,
    o_num_write_record_count  out number,
    o_num_reject_record_count out number
)
is
begin
    sp_load_dim_time(
        i_start_date => i_dat_step_start_date,
        i_end_date => i_dat_step_end_date
    );

    commit;
end;


procedure sp_load_dim_time(
    i_start_date date,
    i_end_date date
)
is
begin
    sp_load_dim_year(i_start_date, i_end_date);
    sp_load_dim_quarter(i_start_date, i_end_date);
    sp_load_dim_month(i_start_date, i_end_date);
    sp_load_dim_week(i_start_date, i_end_date);
    sp_load_dim_date(i_start_date, i_end_date);
    commit;
end; --sp_load_dim_time


procedure sp_load_dim_year(
    i_start_date date,
    i_end_date date
)
is
    l_date date;
begin
    l_date := trunc(i_start_date, 'YEAR');

    while l_date <= i_end_date loop
        merge into dim_year ldyr
        using (select
                   to_number(to_char(l_date, 'YYYY')) as year_id
               from dual) sel
        on (sel.year_id = ldyr.year_id)
        when not matched then
            insert(
                year_id,
                year_name,
                previous_year_id,
                next_year_id
            )
            values(
                sel.year_id,
                to_char(sel.year_id),
                sel.year_id - 1,
                sel.year_id + 1
            );

        l_date := add_months(l_date, 12);
    end loop;
    commit;
end; -- sp_load_dim_year


procedure sp_load_dim_quarter(
    i_start_date date,
    i_end_date date
)
is
    l_date date;
begin
    l_date := trunc(i_start_date, 'Q');

    while l_date <= i_end_date loop
        merge into dim_quarter ldqu
        using (select
                   to_number(to_char(l_date, 'YYYYQ')) as quarter_id
               from dual) sel
        on (sel.quarter_id = ldqu.quarter_id)
        when not matched then
            insert(
                quarter_id,
                quarter_name,
                quarter_in_year,
                previous_quarter_id,
                next_quarter_id,
                year_id
            )
            values(
                sel.quarter_id,
                'Q' || to_char(l_date, 'Q'),
                to_number(to_char(l_date, 'Q')),
                to_number(to_char(add_months(l_date, -3), 'YYYYQ')),
                to_number(to_char(add_months(l_date, 3), 'YYYYQ')),
                to_number(to_char(l_date, 'YYYY'))
            );

        l_date := add_months(l_date, 3);
    end loop;
    commit;
end; -- sp_load_dim_quarter


procedure sp_load_dim_month(
    i_start_date date,
    i_end_date date
)
is
    l_date date;
begin
    l_date := trunc(i_start_date, 'MONTH');

    while l_date <= i_end_date loop
        merge into dim_month ldmo
        using (select
                   to_number(to_char(l_date, 'YYYYMM')) as month_id
               from dual) sel
        on (sel.month_id = ldmo.month_id)
        when not matched then
            insert(
                month_id,
                month_name,
                month_abbrev,
                month_in_year,
                previous_month_id,
                next_month_id,
                quarter_id
            )
            values(
                sel.month_id,
                to_char(l_date, 'MONTH'),
                to_char(l_date, 'MON'),
                to_number(to_char(l_date, 'MM')),
                to_number(to_char(add_months(l_date, -1), 'YYYYMM')),
                to_number(to_char(add_months(l_date, 1), 'YYYYMM')),
                to_number(to_char(l_date, 'YYYYQ'))
            );

        l_date:= add_months(l_date, 1);
    end loop;
    commit;
    null;
end; -- sp_load_dim_month


procedure sp_load_dim_week(
    i_start_date date,
    i_end_date date
)
is
    l_date date;
begin
    l_date := i_start_date;
    while l_date <= i_end_date loop
        merge into dim_week ldwk
        using (select
                   to_number(to_char(l_date, 'YYYYMMDD')) as week_id
               from dual) sel
        on (sel.week_id = ldwk.week_id)
        when not matched then
            insert(
                week_id,
                previous_week_id,
                next_week_id,
                start_day_of_week,  -- sun = 1, mon = 2, ...
                start_day_abbrev,
                starting_month_id,
                ending_month_id
            )
            values(
                sel.week_id,
                to_number(to_char(l_date - 7, 'YYYYMMDD')),
                to_number(to_char(l_date + 7, 'YYYYMMDD')),
                to_number(to_char(l_date, 'D')),
                to_char(l_date, 'DY'),
                to_number(to_char(l_date, 'YYYYMM')),
                to_number(to_char(l_date + 6, 'YYYYMM'))
            );

        l_date := l_date + 1;
    end loop;
    commit;
end; -- sp_load_dim_week


procedure sp_load_dim_date(
    i_start_date date,
    i_end_date date
)
is
    l_date        date;
    l_day_in_week number;
begin
    l_date := i_start_date;
    while l_date <= i_end_date loop
        l_day_in_week := to_number(to_char(l_date, 'D'));
        merge into dim_date lddt
        using (select
                   to_number(to_char(l_date, 'YYYYMMDD')) as date_id
               from dual) sel
        on (sel.date_id = lddt.date_id)
        when not matched then
            insert(
                date_id,
                day_name,
                day_abbrev,
                previous_date_id,
                next_date_id,
                day_in_month,
                day_in_year,
                month_id,
                sun_week_id,
                mon_week_id,
                tue_week_id,
                wed_week_id,
                thu_week_id,
                fri_week_id,
                sat_week_id,
                calendar_date
            )
            values(
                sel.date_id,
                to_char(l_date, 'DAY'),
                to_char(l_date, 'DY'),
                to_number(to_char(l_date - 1, 'YYYYMMDD')),
                to_number(to_char(l_date + 1, 'YYYYMMDD')),
                to_number(to_char(l_date, 'DD')),
                to_number(to_char(l_date, 'DDD')),
                to_number(to_char(l_date, 'YYYYMM')),
                to_number(to_char(l_date - mod(l_day_in_week + 6, 7), 'YYYYMMDD')),
                to_number(to_char(l_date - mod(l_day_in_week + 5, 7), 'YYYYMMDD')),
                to_number(to_char(l_date - mod(l_day_in_week + 4, 7), 'YYYYMMDD')),
                to_number(to_char(l_date - mod(l_day_in_week + 3, 7), 'YYYYMMDD')),
                to_number(to_char(l_date - mod(l_day_in_week + 2, 7), 'YYYYMMDD')),
                to_number(to_char(l_date - mod(l_day_in_week + 1, 7), 'YYYYMMDD')),
                to_number(to_char(l_date - mod(l_day_in_week + 0, 7), 'YYYYMMDD')),
                to_date(to_char(l_date, 'MM/DD/YYYY'),'MM/DD/YYYY')
            );

    l_date := l_date + 1;
    end loop;
    commit;
end; -- sp_load_dim_date


procedure sp_drop_all_data
is
begin
    execute immediate 'truncate table dim_year';
    execute immediate 'truncate table dim_quarter';
    execute immediate 'truncate table dim_month';
    execute immediate 'truncate table dim_week';
    execute immediate 'truncate table dim_date';
    commit;
end; -- sp_drop_all_data


END PA_LOAD_DATE; 
/
