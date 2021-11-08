*&---------------------------------------------------------------------*
*& Report zamq_daemon_monitor
*&---------------------------------------------------------------------*
*& Daemon monitor
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
REPORT zamq_daemon_monitor.

CLASS lcl_app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    DATA: screen_fields TYPE zamq_screen.

    METHODS main.

    METHODS status_9000.

    METHODS status_9005.
    METHODS exit_command_9005.
    METHODS user_command_9005.
    METHODS check_handler_class.

    METHODS status_9010.
    METHODS exit_command.
    METHODS user_command.

    METHODS status_9015.
    METHODS exit_command_9015.
    METHODS user_command_9015.

    METHODS status_9020.
    METHODS exit_command_9020.
    METHODS user_command_9020.

  PRIVATE SECTION.
    DATA daemons TYPE STANDARD TABLE OF zamq_daemons WITH EMPTY KEY.
    DATA alv_daemons TYPE REF TO cl_salv_table.
    DATA broker TYPE STANDARD TABLE OF zamq_broker WITH EMPTY KEY.
    DATA alv_broker TYPE REF TO cl_salv_table.

    DATA canceled TYPE abap_bool.
    DATA change_ind TYPE cdchngind.

    METHODS get_daemons
      RETURNING VALUE(r_result) LIKE daemons.
    METHODS get_brokers
      RETURNING VALUE(r_result) LIKE broker.
    METHODS create_alv_daemons
      CHANGING  c_daemons       LIKE daemons
      RETURNING VALUE(r_result) TYPE REF TO cl_salv_table.
    METHODS create_alv_broker
      CHANGING  c_broker        LIKE broker
      RETURNING VALUE(r_result) TYPE REF TO cl_salv_table.
    METHODS toggle_activation.
    METHODS activate_daemon
      IMPORTING i_daemon TYPE REF TO zamq_daemons.
    METHODS deactivate_daemon
      IMPORTING i_daemon TYPE REF TO zamq_daemons.
    METHODS get_broker
      IMPORTING i_brokername    TYPE zamq_broker_name
      RETURNING VALUE(r_result) TYPE zamq_broker.
    METHODS insert_broker
      RAISING zcx_amq_daemon.
    METHODS get_selected_broker
      RETURNING VALUE(r_result) TYPE zamq_broker.
    METHODS delete_broker.
    METHODS update_broker.
    METHODS insert_daemon.
    METHODS update_daemon.
    METHODS delete_daemon.
    METHODS get_selected_daemon
      RETURNING VALUE(r_result) TYPE zamq_daemons.
    METHODS is_if_implemented
      IMPORTING i_class_name     TYPE seoclsname
                i_interface_name TYPE seoclsname DEFAULT 'ZIF_AMQ_DAEMON'
      RETURNING VALUE(r_result)  TYPE abap_bool.



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

      excluded = VALUE #( ( 'DAEMONS' ) ).
      SET PF-STATUS '9000' EXCLUDING excluded.
      SET TITLEBAR '9000'.

      IF alv_daemons IS NOT BOUND.
        daemons = get_daemons( ).
        alv_daemons = create_alv_daemons( CHANGING c_daemons = daemons ).
      ENDIF.

    ENDIF.

  ENDMETHOD.

  METHOD user_command.

    CASE sy-ucomm.
      WHEN 'BACK' OR 'EXIT'.
        LEAVE PROGRAM.
      WHEN 'DAEMONS'.
        LEAVE TO SCREEN 9000.
      WHEN 'BROKER'.
        LEAVE TO SCREEN 9010.
      WHEN 'TOGGLE'.
        toggle_activation( ).

      WHEN 'NEW'.
        change_ind = 'I'.
        CASE sy-pfkey.
          WHEN '9000'.          "daemons
            CLEAR screen_fields-daemon.
            screen_fields-daemon-bgr_user = sy-uname.
            screen_fields-daemon-stop_message = 'STOP'.
            CALL SCREEN 9005
             STARTING AT 1 1.
          WHEN '9010'.          "Broker
            CLEAR screen_fields-broker.
            CALL SCREEN 9015
             STARTING AT 1 1.
        ENDCASE.

      WHEN 'EDIT'.
        change_ind = 'U'.
        CASE sy-pfkey.
          WHEN '9000'.          "daemons
            screen_fields-daemon = get_selected_daemon( ).
            CHECK screen_fields-daemon IS NOT INITIAL.
            IF screen_fields-daemon-active = icon_oo_object.
              MESSAGE i005(zamq_daemon) WITH screen_fields-daemon-daemon_name.
              RETURN.
            ENDIF.
            CALL SCREEN 9005
             STARTING AT 1 1.
          WHEN '9010'.          "Broker
            screen_fields-broker = get_selected_broker( ).
            CALL SCREEN 9015
             STARTING AT 1 1.
        ENDCASE.

      WHEN 'DELETE_ROW'.
        change_ind = 'D'.
        CASE sy-pfkey.
          WHEN '9000'.          "daemons
            screen_fields-daemon = get_selected_daemon( ).
            CHECK screen_fields-daemon IS NOT INITIAL.
            IF screen_fields-daemon-active = icon_oo_object.
              MESSAGE i005(zamq_daemon) WITH screen_fields-daemon-daemon_name.
              RETURN.
            ENDIF.
            CALL SCREEN 9005
             STARTING AT 1 1.
          WHEN '9010'.          "Broker
            screen_fields-broker = get_selected_broker( ).
            CHECK screen_fields-broker IS NOT INITIAL.

            SELECT FROM zamq_daemons
              FIELDS @abap_true
              WHERE broker_name = @screen_fields-broker-broker_name
              INTO @DATA(broker_in_use)
              UP TO 1 ROWS.
              EXIT.                    "just for abapLint
            ENDSELECT.

            IF broker_in_use = abap_true.
              MESSAGE i006(zamq_daemon) WITH screen_fields-broker-broker_name.
              RETURN.
            ENDIF.

            CALL SCREEN 9015
             STARTING AT 1 1.
        ENDCASE.

    ENDCASE.


  ENDMETHOD.

  METHOD get_daemons.

    SELECT FROM zamq_daemons
      FIELDS *
      ORDER BY broker_name, daemon_name
      INTO TABLE @r_result.

  ENDMETHOD.

  METHOD create_alv_daemons.

    TRY.
        cl_salv_table=>factory(
          EXPORTING
            r_container  = NEW cl_gui_custom_container( 'CCC_9000' )
          IMPORTING
            r_salv_table = r_result
          CHANGING
            t_table      = c_daemons
        ).

        r_result->get_columns( )->get_column( 'GUID' )->set_technical( abap_true ).
        r_result->get_columns( )->get_column( 'MANDT' )->set_technical( abap_true ).
        r_result->get_columns( )->set_optimize( abap_true ).

        r_result->get_display_settings( )->set_striped_pattern( abap_true ).

      CATCH cx_salv_msg
            cx_salv_not_found INTO DATA(lcx).
        MESSAGE lcx TYPE 'I'.
    ENDTRY.

    r_result->display( ).

  ENDMETHOD.

  METHOD get_brokers.

    SELECT FROM zamq_broker
      FIELDS *
      ORDER BY broker_name
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

      excluded = VALUE #( ( 'BROKER' )  ( 'TOGGLE' ) ).
      SET PF-STATUS '9010' EXCLUDING excluded.
      SET TITLEBAR '9010'.

      IF alv_broker IS NOT BOUND.
        broker = get_brokers( ).
        alv_broker = create_alv_broker( CHANGING c_broker = broker ).
      ENDIF.

    ENDIF.

  ENDMETHOD.


  METHOD toggle_activation.

    alv_daemons->get_metadata( ).        "needed after PAI
    DATA(rows) = alv_daemons->get_selections( )->get_selected_rows( ).

    CHECK rows IS NOT INITIAL.

    DATA(daemon) = REF #( daemons[ rows[ 1 ] ] ).

    IF daemon->active = icon_dummy.
      activate_daemon( daemon ).
    ELSE.
      deactivate_daemon( daemon ).
    ENDIF.

    alv_daemons->refresh( ).

  ENDMETHOD.


  METHOD activate_daemon.

    CLEAR screen_fields.
    CALL SCREEN 9020 STARTING AT 5 5.

    IF canceled = abap_false.
      i_daemon->active = icon_oo_object.

      UPDATE zamq_daemons
        SET active = @icon_oo_object
        WHERE guid = @i_daemon->guid.

      CALL FUNCTION 'Z_AMQ_START_DAEMON'
        STARTING NEW TASK 'START_DAEMON'
        EXPORTING
          i_dguid    = i_daemon->guid
          i_stop     = i_daemon->stop_message
          i_user     = app->screen_fields-broker_user
          i_password = app->screen_fields-broker_password.
    ENDIF.

  ENDMETHOD.


  METHOD deactivate_daemon.

    CLEAR screen_fields.
    CALL SCREEN 9020 STARTING AT 5 5.

    IF canceled = abap_false.

      DATA(broker) = get_broker( i_daemon->broker_name ).

      TRY.
          DATA(transport) = zcl_mqtt_transport_tcp=>create(
            iv_host = broker-broker_host
            iv_port = CONV #( broker-broker_port )
            iv_protocol = SWITCH #( broker-use_ssl
                            WHEN abap_true
                            THEN cl_apc_tcp_client_manager=>co_protocol_type_tcps
                            ELSE cl_apc_tcp_client_manager=>co_protocol_type_tcp
                          )
            ).

          transport->connect( ).
          transport->send( NEW zcl_mqtt_packet_connect( iv_username = app->screen_fields-broker_user iv_password = app->screen_fields-broker_password ) ).

          DATA(connack) = CAST zcl_mqtt_packet_connack( transport->listen( 10 ) ).

          IF connack->get_return_code( ) = '00'.
            SPLIT i_daemon->topics AT ',' INTO DATA(first_topic) DATA(dummy).

            REPLACE FIRST OCCURRENCE OF '+' IN first_topic WITH 'dummy'.   "no stop message to generic topic possible -> replace with dummy

            DATA(message) = VALUE zif_mqtt_packet=>ty_message(
              topic   = first_topic
              message = cl_binary_convert=>string_to_xstring_utf8( |{ i_daemon->stop_message }| ) ).

            transport->send( NEW zcl_mqtt_packet_publish( is_message = message ) ).

            transport->send( NEW zcl_mqtt_packet_disconnect( ) ).
            transport->disconnect( ).
          ENDIF.

        CATCH zcx_mqtt_timeout.
          cl_demo_output=>write( 'timeout' ).

        CATCH cx_apc_error
               zcx_mqtt INTO DATA(lcx).
          cl_demo_output=>write( lcx->get_text( ) ).

      ENDTRY.

      i_daemon->active = icon_dummy.

      UPDATE zamq_daemons
        SET active = @icon_dummy
        WHERE guid = @i_daemon->guid.

    ENDIF.

  ENDMETHOD.

  METHOD exit_command_9020.
    canceled = abap_true.
    LEAVE TO SCREEN 0.
  ENDMETHOD.

  METHOD status_9020.

    IF sy-pfkey <> '9020'.
      SET PF-STATUS '9020'.
      SET TITLEBAR '9020'.
    ENDIF.

  ENDMETHOD.

  METHOD status_9015.

    IF sy-pfkey <> '9015'.
      IF change_ind = 'I' OR change_ind = 'U'.
        SET PF-STATUS '9015' EXCLUDING 'DELETE'.
        IF change_ind = 'I'.
          screen_fields-broker-broker_port = '1883'.    "Default Port
        ENDIF.
      ELSE.
        SET PF-STATUS '9015' EXCLUDING 'SAVE'.
      ENDIF.

      LOOP AT SCREEN.
        IF change_ind = 'U'
        OR change_ind = 'D'.
          IF screen-group1 = 'KEY'.
            screen-input = 0.
            MODIFY SCREEN.
          ENDIF.
        ENDIF.

        IF change_ind = 'D'.
          IF screen-group1 = 'FLD'.
            screen-input = 0.
            MODIFY SCREEN.
          ENDIF.
        ENDIF.
      ENDLOOP.

      SET TITLEBAR '9015'.
    ENDIF.

  ENDMETHOD.

  METHOD user_command_9020.
    canceled = abap_false.
    LEAVE TO SCREEN 0.
  ENDMETHOD.

  METHOD get_broker.

    SELECT SINGLE FROM zamq_broker
      FIELDS *
      WHERE broker_name = @i_brokername
      INTO @r_result.

  ENDMETHOD.

  METHOD exit_command_9005.
    LEAVE TO SCREEN 0.
  ENDMETHOD.

  METHOD exit_command_9015.
    LEAVE TO SCREEN 0.
  ENDMETHOD.

  METHOD status_9005.

    IF sy-pfkey <> '9005'.
      IF change_ind = 'I' OR change_ind = 'U'.
        SET PF-STATUS '9005' EXCLUDING 'DELETE'.
      ELSE.
        SET PF-STATUS '9005' EXCLUDING 'SAVE'.
      ENDIF.

      IF change_ind = 'D'.
        LOOP AT SCREEN.
          IF screen-group1 = 'FLD'.
            screen-input = 0.
            MODIFY SCREEN.
          ENDIF.
        ENDLOOP.
      ENDIF.

      SET TITLEBAR '9005'.
    ENDIF.


  ENDMETHOD.

  METHOD user_command_9005.

    TRY.
        CASE change_ind.
          WHEN 'I'.
            insert_daemon( ).
          WHEN 'U'.
            update_daemon( ).
          WHEN 'D'.
            delete_daemon( ).
        ENDCASE.

      CATCH zcx_amq_daemon INTO DATA(lcx).
        MESSAGE lcx TYPE 'W' DISPLAY LIKE 'E'.
        RETURN.
    ENDTRY.

    alv_daemons->refresh( ).
    LEAVE TO SCREEN 0.

  ENDMETHOD.

  METHOD user_command_9015.

    TRY.
        CASE change_ind.
          WHEN 'I'.
            insert_broker( ).
          WHEN 'U'.
            update_broker( ).
          WHEN 'D'.
            delete_broker( ).
        ENDCASE.

      CATCH zcx_amq_daemon INTO DATA(lcx).
        MESSAGE lcx TYPE 'W' DISPLAY LIKE 'E'.
        RETURN.
    ENDTRY.

    alv_broker->refresh( ).
    LEAVE TO SCREEN 0.

  ENDMETHOD.


  METHOD insert_broker.

    SELECT SINGLE FROM zamq_broker
      FIELDS @abap_true
      WHERE broker_name = @screen_fields-broker-broker_name
      INTO @DATA(exists).

    IF exists = abap_true.
      RAISE EXCEPTION TYPE zcx_amq_daemon
        EXPORTING
          textid      = zcx_amq_daemon=>broker_exists
          broker_name = screen_fields-broker-broker_name.
    ENDIF.

    TRANSLATE screen_fields-broker-broker_host TO LOWER CASE.
    INSERT zamq_broker FROM @screen_fields-broker.
    INSERT screen_fields-broker INTO TABLE broker.

  ENDMETHOD.


  METHOD get_selected_broker.

    CLEAR r_result.

    alv_broker->get_metadata( ).        "needed after PAI
    DATA(rows) = alv_broker->get_selections( )->get_selected_rows( ).
    CHECK rows IS NOT INITIAL.

    r_result = broker[ rows[ 1 ] ].

  ENDMETHOD.


  METHOD delete_broker.

    DELETE FROM zamq_broker
      WHERE broker_name = @screen_fields-broker-broker_name.

    DELETE broker
      WHERE broker_name = screen_fields-broker-broker_name.

  ENDMETHOD.


  METHOD update_broker.

    UPDATE zamq_broker FROM @screen_fields-broker.

    DATA(broker_line) = REF #( broker[ broker_name = screen_fields-broker-broker_name ] ).
    broker_line->broker_host = screen_fields-broker-broker_host.
    broker_line->broker_port = screen_fields-broker-broker_port.
    broker_line->use_ssl     = screen_fields-broker-use_ssl.

  ENDMETHOD.


  METHOD insert_daemon.

    TRY.
        screen_fields-daemon-guid = cl_system_uuid=>create_uuid_x16_static( ).
      CATCH cx_uuid_error ##no_handler.
    ENDTRY.

    screen_fields-daemon-active = icon_dummy.
    INSERT zamq_daemons FROM @screen_fields-daemon.
    INSERT screen_fields-daemon INTO TABLE daemons.

  ENDMETHOD.


  METHOD update_daemon.

    UPDATE zamq_daemons FROM @screen_fields-daemon.

    DATA(daemon_line) = REF #( daemons[ guid = screen_fields-daemon-guid ] ).
    daemon_line->* = CORRESPONDING #( screen_fields-daemon ).

  ENDMETHOD.


  METHOD delete_daemon.

    "Todo: don't delete active daemons
    DELETE FROM zamq_daemons
      WHERE guid = @screen_fields-daemon-guid.

    DELETE daemons
      WHERE guid = screen_fields-daemon-guid.

  ENDMETHOD.


  METHOD get_selected_daemon.

    CLEAR r_result.

    alv_daemons->get_metadata( ).        "needed after PAI
    DATA(rows) = alv_daemons->get_selections( )->get_selected_rows( ).
    CHECK rows IS NOT INITIAL.

    r_result = daemons[ rows[ 1 ] ].

  ENDMETHOD.

  METHOD check_handler_class.

    IF NOT is_if_implemented( screen_fields-daemon-handler_class ).
      "Interface ZIF_AMQ_daemon not implemented in class &1
      MESSAGE e007(zamq_daemon) WITH screen_fields-daemon-handler_class.
    ENDIF.

  ENDMETHOD.

  METHOD is_if_implemented.

    SELECT SINGLE FROM vseoimplem
      FIELDS @abap_true
      WHERE clsname = @i_class_name
      AND   refclsname = @i_interface_name
      INTO @r_result.

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

MODULE status_9005 OUTPUT.
  app->status_9005( ).
ENDMODULE.

MODULE exit_command_9005 INPUT.
  app->exit_command_9005( ).
ENDMODULE.

MODULE user_command_9005 INPUT.
  app->user_command_9005( ).
ENDMODULE.

MODULE status_9010 OUTPUT.
  app->status_9010( ).
ENDMODULE.

MODULE status_9020 OUTPUT.
  app->status_9020( ).
ENDMODULE.

MODULE exit_command_9020 INPUT.
  app->exit_command_9020( ).
ENDMODULE.

MODULE user_command_9020 INPUT.
  app->user_command_9020( ).
ENDMODULE.

MODULE status_9015 OUTPUT.
  app->status_9015( ).
ENDMODULE.

MODULE exit_command_9015 INPUT.
  app->exit_command_9015( ).
ENDMODULE.

MODULE user_command_9015 INPUT.
  app->user_command_9015( ).
ENDMODULE.

MODULE check_handler_class INPUT.
  app->check_handler_class( ).
ENDMODULE.
