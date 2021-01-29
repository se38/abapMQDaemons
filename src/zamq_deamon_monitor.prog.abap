*&---------------------------------------------------------------------*
*& Report zamq_deamon_monitor
*&---------------------------------------------------------------------*
*& Deamon monitor
*&---------------------------------------------------------------------*
********************************************************************************
* The MIT License (MIT)
*
* Copyright (c) 2021 Uwe Fetzer and Contributors
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
********************************************************************************
REPORT zamq_deamon_monitor.

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.
  PROTECTED SECTION.
  PRIVATE SECTION.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.



    SELECT * FROM zamq_deamons
      INTO TABLE @DATA(deamons).

    TRY.
        cl_salv_table=>factory(
          IMPORTING
            r_salv_table   = DATA(alv)
          CHANGING
            t_table        = deamons
        ).

        alv->get_columns( )->get_column( 'GUID' )->set_technical( abap_true ).
        alv->get_columns( )->get_column( 'MANDT' )->set_technical( abap_true ).
        alv->get_columns( )->set_optimize( abap_true ).

        alv->get_display_settings( )->set_striped_pattern( abap_true ).

      CATCH cx_salv_msg
            cx_salv_not_found.
    ENDTRY.



    alv->display( ).

  ENDMETHOD.

ENDCLASS.
