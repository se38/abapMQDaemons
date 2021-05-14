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
    DATA deamons TYPE STANDARD TABLE OF zamq_deamons WITH EMPTY KEY.
    DATA alv_deamons TYPE REF TO cl_salv_table.
    DATA broker TYPE STANDARD TABLE OF zamq_broker WITH EMPTY KEY.
    DATA alv_broker TYPE REF TO cl_salv_table.

    DATA canceled TYPE abap_bool.
    DATA change_ind TYPE cdchngind.

    METHODS get_deamons
      RETURNING VALUE(r_result) LIKE deamons.
    METHODS get_brokers
      RETURNING VALUE(r_result) LIKE broker.
    METHODS create_alv_deamons
      CHANGING  c_deamons       LIKE deamons
      RETURNING VALUE(r_result) TYPE REF TO cl_salv_table.
    METHODS create_alv_broker
      CHANGING  c_broker        LIKE broker
      RETURNING VALUE(r_result) TYPE REF TO cl_salv_table.
    METHODS toggle_activation.
    METHODS activate_deamon
      IMPORTING i_deamon TYPE REF TO zamq_deamons.
    METHODS deactivate_deamon
      IMPORTING i_deamon TYPE REF TO zamq_deamons.
    METHODS get_broker
      IMPORTING i_brokername    TYPE zamq_broker_name
      RETURNING VALUE(r_result) TYPE zamq_broker.
    METHODS insert_broker
      RAISING zcx_amq_deamon.
    METHODS get_selected_broker
      RETURNING VALUE(r_result) TYPE zamq_broker.
    METHODS delete_broker.
    METHODS update_broker.
    METHODS insert_deamon.
    METHODS update_deamon.
    METHODS delete_deamon.
    METHODS get_selected_deamon
      RETURNING
        VALUE(r_result) TYPE zamq_deamons.

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

    CASE sy-ucomm.
      WHEN 'BACK' OR 'EXIT'.
        LEAVE PROGRAM.
      WHEN 'DEAMONS'.
        LEAVE TO SCREEN 9000.
      WHEN 'BROKER'.
        LEAVE TO SCREEN 9010.
      WHEN 'TOGGLE'.
        toggle_activation( ).

      WHEN 'NEW'.
        change_ind = 'I'.
        CASE sy-pfkey.
          WHEN '9000'.          "Deamons
            CLEAR screen_fields-deamon.
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
          WHEN '9000'.          "Deamons
            screen_fields-deamon = get_selected_deamon( ).
            CHECK screen_fields-deamon IS NOT INITIAL.
            IF screen_fields-deamon-active = icon_oo_object.
              MESSAGE i005(zamq_deamon) WITH screen_fields-deamon-deamon_name.
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
          WHEN '9000'.          "Deamons
            screen_fields-deamon = get_selected_deamon( ).
            CHECK screen_fields-deamon IS NOT INITIAL.
            IF screen_fields-deamon-active = icon_oo_object.
              MESSAGE i005(zamq_deamon) WITH screen_fields-deamon-deamon_name.
              RETURN.
            ENDIF.
            CALL SCREEN 9005
             STARTING AT 1 1.
          WHEN '9010'.          "Broker
            screen_fields-broker = get_selected_broker( ).
            CHECK screen_fields-broker IS NOT INITIAL.

            SELECT FROM zamq_deamons
              FIELDS @abap_true
              WHERE broker_name = @screen_fields-broker-broker_name
              INTO @DATA(broker_in_use)
              UP TO 1 ROWS.
            ENDSELECT.

            IF broker_in_use = abap_true.
              MESSAGE i006(zamq_deamon) WITH screen_fields-broker-broker_name.
              RETURN.
            ENDIF.

            CALL SCREEN 9015
             STARTING AT 1 1.
        ENDCASE.

    ENDCASE.


  ENDMETHOD.

  METHOD get_deamons.

    SELECT FROM zamq_deamons
      FIELDS *
      ORDER BY broker_name, deamon_name
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
        r_result->get_columns( )->get_column( 'LOG_HANDLE' )->set_technical( abap_true ).
        r_result->get_columns( )->set_optimize( abap_true ).

        r_result->get_display_settings( )->set_striped_pattern( abap_true ).

      CATCH cx_salv_msg
            cx_salv_not_found.
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

    alv_deamons->get_metadata( ).        "needed after PAI
    DATA(rows) = alv_deamons->get_selections( )->get_selected_rows( ).

    CHECK rows IS NOT INITIAL.

    DATA(deamon) = REF #( deamons[ rows[ 1 ] ] ).

    IF deamon->active = icon_dummy.
      activate_deamon( deamon ).
    ELSE.
      deactivate_deamon( deamon ).
    ENDIF.

    alv_deamons->refresh( ).

  ENDMETHOD.


  METHOD activate_deamon.

    CLEAR screen_fields.
    CALL SCREEN 9020 STARTING AT 5 5.

    IF canceled = abap_false.
      i_deamon->active = icon_oo_object.

      UPDATE zamq_deamons
        SET active = @icon_oo_object
        WHERE guid = @i_deamon->guid.

      CALL FUNCTION 'Z_AMQ_START_DEAMON'
        STARTING NEW TASK 'START_DEAMON'
        EXPORTING
          i_dguid    = i_deamon->guid
          i_user     = app->screen_fields-broker_user
          i_password = app->screen_fields-broker_password.
    ENDIF.

  ENDMETHOD.


  METHOD deactivate_deamon.

    CLEAR screen_fields.
    CALL SCREEN 9020 STARTING AT 5 5.

    IF canceled = abap_false.

      DATA(broker) = get_broker( i_deamon->broker_name ).

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
            SPLIT i_deamon->topics AT ',' INTO DATA(first_topic) DATA(dummy).
            TRANSLATE first_topic USING '/../+**+'.

            DATA(message) = VALUE zif_mqtt_packet=>ty_message(
              topic   = first_topic
              message = cl_binary_convert=>string_to_xstring_utf8( 'STOP' ) ).

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

      i_deamon->active = icon_dummy.

      UPDATE zamq_deamons
        SET active = @icon_dummy
        WHERE guid = @i_deamon->guid.

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
            insert_deamon( ).
          WHEN 'U'.
            update_deamon( ).
          WHEN 'D'.
            delete_deamon( ).
        ENDCASE.

      CATCH zcx_amq_deamon INTO DATA(lcx).
        MESSAGE lcx TYPE 'W' DISPLAY LIKE 'E'.
        RETURN.
    ENDTRY.

    alv_deamons->refresh( ).
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

      CATCH zcx_amq_deamon INTO DATA(lcx).
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
      RAISE EXCEPTION TYPE zcx_amq_deamon
        EXPORTING
          textid      = zcx_amq_deamon=>broker_exists
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


  METHOD insert_deamon.

    CALL FUNCTION 'GUID_CREATE'
      IMPORTING
        ev_guid_16 = screen_fields-deamon-guid.

    screen_fields-deamon-active = icon_dummy.
    INSERT zamq_deamons FROM @screen_fields-deamon.
    INSERT screen_fields-deamon INTO TABLE deamons.

  ENDMETHOD.


  METHOD update_deamon.

    UPDATE zamq_deamons FROM @screen_fields-deamon.

    DATA(deamon_line) = REF #( deamons[ guid = screen_fields-deamon-guid ] ).
    deamon_line->broker_name = screen_fields-deamon-broker_name.
    deamon_line->deamon_name = screen_fields-deamon-deamon_name.
    deamon_line->topics = screen_fields-deamon-topics.
    deamon_line->handler_class = screen_fields-deamon-handler_class.

  ENDMETHOD.


  METHOD delete_deamon.

    "Todo: don't delete active deamons
    DELETE FROM zamq_deamons
      WHERE guid = @screen_fields-deamon-guid.

    DELETE deamons
      WHERE guid = screen_fields-deamon-guid.

  ENDMETHOD.


  METHOD get_selected_deamon.

    CLEAR r_result.

    alv_deamons->get_metadata( ).        "needed after PAI
    DATA(rows) = alv_deamons->get_selections( )->get_selected_rows( ).
    CHECK rows IS NOT INITIAL.

    r_result = deamons[ rows[ 1 ] ].

  ENDMETHOD.

  METHOD check_handler_class.

    IF NOT NEW zcl_amq_deamon_helper( )->is_if_implemented( screen_fields-deamon-handler_class ).
      "Interface ZIF_AMQ_DEAMON not implented in class &1
      MESSAGE e007(zamq_deamon) WITH screen_fields-deamon-handler_class.
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
