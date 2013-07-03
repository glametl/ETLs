create or replace package pkg_compose_email as

  procedure run_email
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2,
    p_to           varchar2
  );

end;
/
create or replace package body pkg_compose_email as

  procedure run_email
  (
    p_run_id       number,
    p_stage        varchar2,
    p_process_date number,
    p_message      varchar2,
    p_process_name varchar2,
    p_to           varchar2
  ) is
    l_sender     varchar2(1000) := 'odsimportreport@glam.com';
    l_to         varchar2(1000);
    l_subject    varchar2(1000) := '';
    l_today      varchar2(8);
    l_crlf       varchar2(2) := chr(13) || chr(10);
    l_hrs        number;
    l_minutes    number;
    l_sec        number;
    l_start_time date;
    l_end_time   date;
    l_complete   number;
    l_message    varchar2(32000);
    l_error_msg  varchar2(4000);
  begin
    l_to    := p_to;
    l_today := to_char(sysdate, 'yyyymmdd');
    select upper(sys_context('USERENV', 'DB_NAME')) || ': ' ||
            p_message || ' Date: ' || p_process_date || ' ' ||
            substr(p_process_name, 1, instr(p_process_name, '-', 1) - 1) ||
            ' -> ETL RUN_ID : ' || p_run_id
    into   l_subject
    from   dual;
    if (upper(p_stage) = 'START') then
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message ||
                   ' load process has Started for ' ||
                   p_process_date || l_crlf ||
                   'Please restrain from running any reports and queries. ' ||
                   l_crlf ||
                   'You will be notified once the process has been completed.';
    elsif upper(p_stage) = 'COMPLETE' then
      select count(1), min(start_time), max(end_time)
      into   l_complete, l_start_time, l_end_time
      from   daily_process_log
      where  run_id = p_run_id
      and    status = 'COMPLETE';
      l_hrs     := trunc(((86400 *
                         (l_end_time - l_start_time)) / 60) / 60) -
                   24 *
                   (trunc((((86400 *
                          (l_end_time - l_start_time)) / 60) / 60) / 24));
      l_minutes := trunc((86400 *
                         (l_end_time - l_start_time)) / 60) -
                   60 *
                   (trunc(((86400 *
                          (l_end_time - l_start_time)) / 60) / 60));
      l_sec     := trunc(86400 *
                         (l_end_time - l_start_time)) -
                   60 *
                   (trunc((86400 *
                          (l_end_time - l_start_time)) / 60));
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID: ' ||
                   p_run_id || l_crlf || 'Data Date : ' ||
                   p_process_date || l_crlf ||
                   'Job Started: ' ||
                   to_char(l_start_time, ' dd Mon YYYY hh24:mi:ss') ||
                   l_crlf || 'Job Ended  : ' ||
                   to_char(l_end_time, ' dd Mon YYYY hh24:mi:ss') ||
                   l_crlf ||
                   'Duration process time (In Hrs) : ' ||
                   l_hrs || l_crlf ||
                   'Duration process time (In Minutes) : ' ||
                   l_minutes || l_crlf ||
                   'Duration process time (In Seconds) : ' ||
                   l_sec || l_crlf ||
                   'Total Time (Hr:Min:Sec) :' || l_hrs || ':' ||
                   l_minutes || ':' || l_sec;
      l_message := l_message || l_crlf || l_crlf ||
                   'Steps Completed: ' || l_complete ||
                   l_crlf || 'All jobs ran successfully ' ||
                   l_crlf || l_crlf || '- Data Team';
      l_subject := l_subject || ' ,Successful on ' ||
                   l_today;
    elsif upper(p_stage) = 'FAIL' then
      select error_reason
      into   l_error_msg
      from   daily_process_log
      where  run_id = p_run_id
      and    process_date = p_process_date
      and    upper(process_name) = upper(p_process_name);
      l_message := l_crlf || 'Hello ,' || l_crlf || l_crlf ||
                   p_message || ' details are as follows: ' ||
                   l_crlf || l_crlf || 'Job Run ID : ' ||
                   p_run_id || l_crlf || l_crlf ||
                   'Data Date : ' || p_process_date ||
                   l_crlf || l_crlf || ' Failed on : ' ||
                   l_today || l_crlf || l_crlf ||
                   ' Failed Reason : ' || l_error_msg ||
                   l_crlf || l_crlf || '- Data Team';
      l_subject := l_subject || ' Failed on ' || l_today;
    end if;
    pa_send_email.mail(l_sender, l_to, l_subject, l_message);
  end;

end;
/
