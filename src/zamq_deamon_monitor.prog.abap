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

CLASS lcl_app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.

    METHODS status_9000.
    METHODS status_9010.
    METHODS exit_command.
    METHODS user_command.

  PRIVATE SECTION.
    DATA deamons TYPE STANDARD TABLE OF zamq_deamons WITH EMPTY KEY.
    DATA alv_deamons TYPE REF TO cl_salv_table.
    DATA broker TYPE STANDARD TABLE OF zamq_broker WITH EMPTY KEY.
    DATA alv_broker TYPE REF TO cl_salv_table.

    METHODS get_deamons
      RETURNING VALUE(r_result) LIKE deamons.
    METHODS get_broker
      RETURNING VALUE(r_result) LIKE broker.

    METHODS create_alv_deamons
      CHANGING  c_deamons       LIKE deamons
      RETURNING VALUE(r_result) TYPE REF TO cl_salv_table.
    METHODS create_alv_broker
      CHANGING  c_broker        LIKE broker
      RETURNING VALUE(r_result) TYPE REF TO cl_salv_table.

ENDCLASS.

DATA(app) = NEW lcl_app( ).
app->main( ).

CLASS lcl_app IMPLEMENTATION.

  METHOD main.
    CALL SCREEN 9000.
  ENDMETHOD.

  METHOD exit_command.
    LEAVE PROGRAM.
  ENDMETHOD.

  METHOD status_9000.

    IF sy-pfkey <> '9000'.
      DATA excluded TYPE STANDARD TABLE OF syst_ucomm WITH EMPTY KEY.

      excluded = VALUE #( ( 'DEAMONS' ) ).
      SET PF-STATUS '9000' EXCLUDING excluded.
      SET TITLEBAR '9000'.

      IF alv_deamons IS NOT BOUND.
        deamons = get_deamons( ).
        alv_deamons = create_alv_deamons( CHANGING c_deamons = deamons ).
      ENDIF.

    ENDIF.

  ENDMETHOD.

  METHOD user_command.

    DATA(user_command) = sy-ucomm.
    CLEAR sy-ucomm.

    CASE user_command.
      WHEN 'BACK' OR 'EXIT'.
        LEAVE PROGRAM.
      WHEN 'DEAMONS'.
        LEAVE TO SCREEN 9000.
      WHEN 'BROKER'.
        LEAVE TO SCREEN 9010.
    ENDCASE.

  ENDMETHOD.

  METHOD get_deamons.

    SELECT * FROM zamq_deamons
      INTO TABLE @r_result.

  ENDMETHOD.

  METHOD create_alv_deamons.

    TRY.
        cl_salv_table=>factory(
          EXPORTING
            r_container  = NEW cl_gui_custom_container( 'CCC_9000' )
          IMPORTING
            r_salv_table = r_result
          CHANGING
            t_table      = c_deamons
        ).

        r_result->get_columns( )->get_column( 'GUID' )->set_technical( abap_true ).
        r_result->get_columns( )->get_column( 'MANDT' )->set_technical( abap_true ).
        r_result->get_columns( )->set_optimize( abap_true ).

        r_result->get_display_settings( )->set_striped_pattern( abap_true ).

      CATCH cx_salv_msg
            cx_salv_not_found.
    ENDTRY.

    r_result->display( ).

  ENDMETHOD.

  METHOD get_broker.

    SELECT * FROM zamq_broker
      INTO TABLE @r_result.

  ENDMETHOD.

  METHOD create_alv_broker.

    TRY.
        cl_salv_table=>factory(
          EXPORTING
            r_container  = NEW cl_gui_custom_container( 'CCC_9010' )
          IMPORTING
            r_salv_table = r_result
          CHANGING
            t_table      = c_broker
        ).

        r_result->get_columns( )->get_column( 'MANDT' )->set_technical( abap_true ).
        r_result->get_columns( )->set_optimize( abap_true ).

        r_result->get_display_settings( )->set_striped_pattern( abap_true ).

      CATCH cx_salv_msg
            cx_salv_not_found.
    ENDTRY.

    r_result->display( ).

  ENDMETHOD.

  METHOD status_9010.

    IF sy-pfkey <> '9010'.
      DATA excluded TYPE STANDARD TABLE OF syst_ucomm WITH EMPTY KEY.

      excluded = VALUE #( ( 'BROKER' )  ( 'ACTIVATE' ) ( 'DEACTIVATE' ) ).
      SET PF-STATUS '9010' EXCLUDING excluded.
      SET TITLEBAR '9010'.

      IF alv_broker IS NOT BOUND.
        broker = get_broker( ).
        alv_broker = create_alv_broker( CHANGING c_broker = broker ).
      ENDIF.

    ENDIF.

  ENDMETHOD.

ENDCLASS.

MODULE status_9000 OUTPUT.
  app->status_9000( ).
ENDMODULE.

MODULE exit_command INPUT.
  app->exit_command( ).
ENDMODULE.

MODULE user_command INPUT.
  app->user_command( ).
ENDMODULE.

MODULE status_9010 OUTPUT.
  app->status_9010( ).
ENDMODULE.
