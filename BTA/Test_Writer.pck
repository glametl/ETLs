CREATE OR REPLACE PACKAGE Test_Writer AS
/******************************************************************************
   NAME:       Load_Segment
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        9/23/2009             1. Created this package.
******************************************************************************/

PROCEDURE Test_Score_Writer( v_datadate IN NUMBER );

END Test_Writer; 
/
CREATE OR REPLACE PACKAGE BODY Test_Writer AS
/******************************************************************************
   NAME:       Load_Segment
   PURPOSE:

   REVISIONS:
   Ver        Date        Author           Description
   ---------  ----------  ---------------  ------------------------------------
   1.0        9/23/2009             1. Created this package body.
******************************************************************************/


PROCEDURE Test_Score_Writer( v_datadate IN NUMBER )
IS
    cursor c1 is SELECT USER_ID || ',' || CATEGORY_LIST AS csv FROM BT_DATA.FACT_BT_DELIVERY;
    type c1type is table of c1%rowtype index by binary_integer;
    v_name    VARCHAR2(128) := 'bt_memcache'||v_datadate||'.txt';
    stage_tab c1type;
    l_output        utl_file.file_type;
    l_buffer        VARCHAR2(32767) := NULL;
    l_new_line      VARCHAR2(10) := chr(10);
begin
    l_output := utl_file.fopen_nchar('BT_OUTPUT', v_name, 'w');

    open c1;
    loop
        fetch c1 bulk collect into stage_tab limit 10000;
         
        for i in 1 .. stage_tab.count loop
               
            IF LENGTH(l_buffer || stage_tab(i).csv) >= 10000
                THEN
                    utl_file.put_line_nchar(l_output, l_buffer);
                    l_buffer := stage_tab(i).csv;
                ELSE
                    IF l_buffer IS NULL THEN
                        l_buffer := stage_tab(i).csv;
                        ELSE
                        l_buffer := l_buffer || l_new_line || stage_tab(i).csv;
                    END IF;
            END IF;
        end loop;
        exit when c1%notfound;
     
    end loop;
    utl_file.put_line_nchar(l_output, l_buffer);
    utl_file.fclose(l_output);
    close c1;
END;

END Test_Writer; 
/
