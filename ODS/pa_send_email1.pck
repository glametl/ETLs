create or replace package pa_send_email1 as

  /******************************************************************************
     NAME:       PA_SEND_EMAIL
     PURPOSE:
  
     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0        8/28/2008             1. Created this package.
  ******************************************************************************/
  smtp_host varchar2(256) := '10.0.1.253';

  smtp_port pls_integer := 25;

  smtp_domain varchar2(256) := '10.0.1.253';

  mailer_id constant varchar2(256) := 'Mailer by Oracle UTL_SMTP';

  boundary constant varchar2(256) := '-----7D81B75CCC90D2974F7A1CBD';

  first_boundary constant varchar2(256) := '--' || boundary ||
                                           utl_tcp.crlf;

  last_boundary constant varchar2(256) := '--' || boundary || '--' ||
                                          utl_tcp.crlf;

  -- A MIME type that denotes multi-part email (MIME) messages.
  multipart_mime_type constant varchar2(256) := 'multipart/mixed; boundary="' ||
                                                boundary || '"';

  max_base64_line_width constant pls_integer := 76 / 4 * 3;

  procedure mail
  (
    sender     in varchar2,
    recipients in varchar2,
    subject    in varchar2,
    message    in varchar2
  );

  function begin_mail
  (
    sender     in varchar2,
    recipients in varchar2,
    subject    in varchar2,
    mime_type  in varchar2 default 'text/plain',
    priority   in pls_integer default null
  ) return utl_smtp.connection;

  -- Write email body 
  procedure write_text
  (
    conn    in out nocopy utl_smtp.connection,
    message in varchar2
  );

  procedure write_mb_text
  (
    conn    in out nocopy utl_smtp.connection,
    message in varchar2
  );

  procedure attach_text
  (
    conn      in out nocopy utl_smtp.connection,
    data      in varchar2,
    mime_type in varchar2 default 'text/plain',
    inline    in boolean default true,
    filename  in varchar2 default null,
    last      in boolean default false
  );

  procedure begin_attachment
  (
    conn         in out nocopy utl_smtp.connection,
    mime_type    in varchar2 default 'text/plain',
    inline       in boolean default true,
    filename     in varchar2 default null,
    transfer_enc in varchar2 default null
  );

  procedure end_attachment
  (
    conn in out nocopy utl_smtp.connection,
    last in boolean default false
  );

  procedure end_mail(conn in out nocopy utl_smtp.connection);

  function begin_session return utl_smtp.connection;

  procedure begin_mail_in_session
  (
    conn       in out nocopy utl_smtp.connection,
    sender     in varchar2,
    recipients in varchar2,
    subject    in varchar2,
    mime_type  in varchar2 default 'text/plain',
    priority   in pls_integer default null
  );

  procedure end_mail_in_session(conn in out nocopy utl_smtp.connection);

  procedure end_session(conn in out nocopy utl_smtp.connection);

  procedure compose_email
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
create or replace package body pa_send_email1 as

  /******************************************************************************
     NAME:       PA_SEND_EMAIL
     PURPOSE:
  
     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0        8/28/2008             1. Created this package body.
  ******************************************************************************/
  function get_address(addr_list in out varchar2)
    return varchar2 is
    addr varchar2(256);
    i    pls_integer;
    function lookup_unquoted_char
    (
      str  in varchar2,
      chrs in varchar2
    ) return pls_integer as
      c            varchar2(5);
      i            pls_integer;
      len          pls_integer;
      inside_quote boolean;
    begin
      inside_quote := false;
      i            := 1;
      len          := length(str);
      while (i <= len) loop
        c := substr(str, i, 1);
        if (inside_quote) then
          if (c = '"') then
            inside_quote := false;
          elsif (c = '\') then
            i := i + 1; -- Skip the quote character
          end if;
          goto next_char;
        end if;
        if (c = '"') then
          inside_quote := true;
          goto next_char;
        end if;
        if (instr(chrs, c) >= 1) then
          return i;
        end if;
        <<next_char>>
        i := i + 1;
      end loop;
      return 0;
    end;
  
  begin
    addr_list := ltrim(addr_list);
    i         := lookup_unquoted_char(addr_list, ',;');
    if (i >= 1) then
      addr      := substr(addr_list, 1, i - 1);
      addr_list := substr(addr_list, i + 1);
    else
      addr      := addr_list;
      addr_list := '';
    end if;
    i := lookup_unquoted_char(addr, '<');
    if (i >= 1) then
      addr := substr(addr, i + 1);
      i    := instr(addr, '>');
      if (i >= 1) then
        addr := substr(addr, 1, i - 1);
      end if;
    end if;
    return addr;
  end;

  -- Write a MIME header
  procedure write_mime_header
  (
    conn  in out nocopy utl_smtp.connection,
    name  in varchar2,
    value in varchar2
  ) is
  begin
    utl_smtp.write_data(conn, name || ': ' || value ||
                         utl_tcp.crlf);
  end;

  -- Mark a message-part boundary.  Set <last> to TRUE for the last boundary.
  procedure write_boundary
  (
    conn in out nocopy utl_smtp.connection,
    last in boolean default false
  ) as
  begin
    if (last) then
      utl_smtp.write_data(conn, last_boundary);
    else
      utl_smtp.write_data(conn, first_boundary);
    end if;
  end;

  ------------------------------------------------------------------------
  procedure mail
  (
    sender     in varchar2,
    recipients in varchar2,
    subject    in varchar2,
    message    in varchar2
  ) is
    conn utl_smtp.connection;
  begin
    conn := begin_mail(sender, recipients, subject);
    write_text(conn, message);
    end_mail(conn);
  end;

  ------------------------------------------------------------------------
  function begin_mail
  (
    sender     in varchar2,
    recipients in varchar2,
    subject    in varchar2,
    mime_type  in varchar2 default 'text/plain',
    priority   in pls_integer default null
  ) return utl_smtp.connection is
    conn utl_smtp.connection;
  begin
    conn := begin_session;
    begin_mail_in_session(conn, sender, recipients, subject, mime_type, priority);
    return conn;
  end;

  ------------------------------------------------------------------------
  procedure write_text
  (
    conn    in out nocopy utl_smtp.connection,
    message in varchar2
  ) is
  begin
    utl_smtp.write_data(conn, message);
  end;

  ------------------------------------------------------------------------
  procedure write_mb_text
  (
    conn    in out nocopy utl_smtp.connection,
    message in varchar2
  ) is
  begin
    utl_smtp.write_raw_data(conn, utl_raw.cast_to_raw(message));
  end;

  ------------------------------------------------------------------------
  procedure write_raw
  (
    conn    in out nocopy utl_smtp.connection,
    message in raw
  ) is
  begin
    utl_smtp.write_raw_data(conn, message);
  end;

  ------------------------------------------------------------------------
  procedure attach_text
  (
    conn      in out nocopy utl_smtp.connection,
    data      in varchar2,
    mime_type in varchar2 default 'text/plain',
    inline    in boolean default true,
    filename  in varchar2 default null,
    last      in boolean default false
  ) is
  begin
    begin_attachment(conn, mime_type, inline, filename);
    write_text(conn, data);
    end_attachment(conn, last);
  end;

  ------------------------------------------------------------------------
  procedure attach_base64
  (
    conn      in out nocopy utl_smtp.connection,
    data      in raw,
    mime_type in varchar2 default 'application/octet',
    inline    in boolean default true,
    filename  in varchar2 default null,
    last      in boolean default false
  ) is
    i   pls_integer;
    len pls_integer;
  begin
    begin_attachment(conn, mime_type, inline, filename, 'base64');
    -- Split the Base64-encoded attachment into multiple lines
    i   := 1;
    len := utl_raw.length(data);
    while (i < len) loop
      if (i + max_base64_line_width < len) then
        utl_smtp.write_raw_data(conn, utl_encode.base64_encode(utl_raw.substr(data, i, max_base64_line_width)));
      else
        utl_smtp.write_raw_data(conn, utl_encode.base64_encode(utl_raw.substr(data, i)));
      end if;
      utl_smtp.write_data(conn, utl_tcp.crlf);
      i := i + max_base64_line_width;
    end loop;
    end_attachment(conn, last);
  end;

  ------------------------------------------------------------------------
  procedure begin_attachment
  (
    conn         in out nocopy utl_smtp.connection,
    mime_type    in varchar2 default 'text/plain',
    inline       in boolean default true,
    filename     in varchar2 default null,
    transfer_enc in varchar2 default null
  ) is
  begin
    write_boundary(conn);
    write_mime_header(conn, 'Content-Type', mime_type);
    if (filename is not null) then
      if (inline) then
        write_mime_header(conn, 'Content-Disposition', 'inline; filename="' ||
                           filename || '"');
      else
        write_mime_header(conn, 'Content-Disposition', 'attachment; filename="' ||
                           filename || '"');
      end if;
    end if;
    if (transfer_enc is not null) then
      write_mime_header(conn, 'Content-Transfer-Encoding', transfer_enc);
    end if;
    utl_smtp.write_data(conn, utl_tcp.crlf);
  end;

  ------------------------------------------------------------------------
  procedure end_attachment
  (
    conn in out nocopy utl_smtp.connection,
    last in boolean default false
  ) is
  begin
    utl_smtp.write_data(conn, utl_tcp.crlf);
    if (last) then
      write_boundary(conn, last);
    end if;
  end;

  ------------------------------------------------------------------------
  procedure end_mail(conn in out nocopy utl_smtp.connection) is
  begin
    end_mail_in_session(conn);
    end_session(conn);
  end;

  ------------------------------------------------------------------------
  function begin_session return utl_smtp.connection is
    conn utl_smtp.connection;
  begin
    -- open SMTP connection
    conn := utl_smtp.open_connection(smtp_host, smtp_port);
    utl_smtp.helo(conn, smtp_domain);
    return conn;
  end;

  ------------------------------------------------------------------------
  procedure begin_mail_in_session
  (
    conn       in out nocopy utl_smtp.connection,
    sender     in varchar2,
    recipients in varchar2,
    subject    in varchar2,
    mime_type  in varchar2 default 'text/plain',
    priority   in pls_integer default null
  ) is
    my_recipients varchar2(32767) := recipients;
    my_sender     varchar2(32767) := sender;
  begin
    -- Specify sender's address (our server allows bogus address
    -- as long as it is a full email address (xxx@yyy.com).
    utl_smtp.mail(conn, get_address(my_sender));
    -- Specify recipient(s) of the email.
    while (my_recipients is not null) loop
      utl_smtp.rcpt(conn, get_address(my_recipients));
    end loop;
    -- Start body of email
    utl_smtp.open_data(conn);
    -- Set "From" MIME header
    write_mime_header(conn, 'From', sender);
    -- Set "To" MIME header
    write_mime_header(conn, 'To', recipients);
    -- Set "Subject" MIME header
    write_mime_header(conn, 'Subject', subject);
    -- Set "Content-Type" MIME header
    write_mime_header(conn, 'Content-Type', mime_type);
    -- Set "X-Mailer" MIME header
    write_mime_header(conn, 'X-Mailer', mailer_id);
    -- Set priority:
    --   High      Normal       Low
    --   1     2     3     4     5
    if (priority is not null) then
      write_mime_header(conn, 'X-Priority', priority);
    end if;
    -- Send an empty line to denotes end of MIME headers and
    -- beginning of message body.
    utl_smtp.write_data(conn, utl_tcp.crlf);
    if (mime_type like 'multipart/mixed%') then
      write_text(conn, 'This is a multi-part message in MIME format.' ||
                  utl_tcp.crlf);
    end if;
  end;

  ------------------------------------------------------------------------
  procedure end_mail_in_session(conn in out nocopy utl_smtp.connection) is
  begin
    utl_smtp.close_data(conn);
  end;

  ------------------------------------------------------------------------
  procedure end_session(conn in out nocopy utl_smtp.connection) is
  begin
    utl_smtp.quit(conn);
  end;

  procedure compose_email
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
      select min(start_time), max(end_time)
      into   l_start_time, l_end_time
      from   daily_process_log
      where  run_id = p_run_id
      and    status = 'COMPLETE'
      and    upper(process_name) = upper(p_process_name);
      select count(1)
      into   l_complete
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
    mail(l_sender, l_to, l_subject, l_message);
  end;

end;
/
