create or replace package pkg_generic_ddl as

  procedure add_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2,
    p_part_val    varchar2
  );

  procedure drop_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  );

  procedure analyze_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  );

  procedure truncate_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  );

  procedure analyze_table
  (
    p_scehma_name varchar2,
    p_table_name  varchar2
  );

  procedure truncate_table
  (
    p_scehma_name varchar2,
    p_table_name  varchar2
  );

  procedure truncate_subpartition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  );

  procedure sql_from_ods(p_sql varchar2);

end;
/
create or replace package body pkg_generic_ddl as

  procedure add_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2,
    p_part_val    varchar2
  ) is
  begin
    begin
      execute immediate 'alter table ' || p_scehma_name || '.' ||
                        p_table_name || ' add partition ' ||
                        p_part_name ||
                        ' values less than (' || p_part_val || ')';
    exception
      when others then
        null;
    end;
  end;

  procedure drop_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  ) is
  begin
    begin
      execute immediate 'alter table ' || p_scehma_name || '.' ||
                        p_table_name || ' drop partition ' ||
                        p_part_name;
    exception
      when others then
        null;
    end;
  end;

  procedure analyze_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  ) is
  begin
    begin
      execute immediate 'analyze table ' || p_scehma_name || '.' ||
                        p_table_name || ' partition ( ' ||
                        p_part_name ||
                        ') estimate statistics';
    exception
      when others then
        null;
    end;
  end;

  procedure truncate_partition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  ) is
  begin
    execute immediate 'alter table ' || p_scehma_name || '.' ||
                      p_table_name ||
                      ' truncate partition ' || p_part_name;
  exception
    when others then
      null;
  end;

  procedure analyze_table
  (
    p_scehma_name varchar2,
    p_table_name  varchar2
  ) is
  begin
    begin
      execute immediate 'analyze table ' || p_scehma_name || '.' ||
                        p_table_name ||
                        ' estimate statistics';
    exception
      when others then
        null;
    end;
  end;

  procedure truncate_table
  (
    p_scehma_name varchar2,
    p_table_name  varchar2
  ) is
  begin
    execute immediate 'truncate table ' || p_scehma_name || '.' ||
                      p_table_name;
  exception
    when others then
      null;
  end;

  procedure truncate_subpartition
  (
    p_scehma_name varchar2,
    p_table_name  varchar2,
    p_part_name   varchar2
  ) is
  begin
    execute immediate 'Alter table ' || p_scehma_name || '.' ||
                      p_table_name ||
                      ' truncate subpartition ' ||
                      p_part_name;
  exception
    when others then
      null;
  end;

  procedure sql_from_ods(p_sql varchar2) is
  begin
    execute immediate p_sql;
  end;

end;
/
