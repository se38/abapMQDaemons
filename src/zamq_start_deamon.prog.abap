*&---------------------------------------------------------------------*
*& Report zamq_start_deamon
*&---------------------------------------------------------------------*
*& abapMQ deamon: wait for messages
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
REPORT zamq_start_deamon.

PARAMETERS: p_dguid TYPE guid_16 OBLIGATORY.
PARAMETERS: p_stop TYPE c LENGTH 20 DEFAULT 'STOP' OBLIGATORY.
PARAMETERS: p_user TYPE zamq_user LOWER CASE.
PARAMETERS: p_pass TYPE zamq_password LOWER CASE.

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.

  PRIVATE SECTION.
    METHODS get_deamon
      IMPORTING i_dguid         TYPE guid_16
      RETURNING VALUE(r_result) TYPE zamq_deamons.
    METHODS get_broker
      IMPORTING i_brokername    TYPE zamq_broker_name
      RETURNING VALUE(r_result) TYPE zamq_broker.
    METHODS create_tcp_transport
      IMPORTING i_broker        TYPE zamq_broker
      RETURNING VALUE(r_result) TYPE REF TO zif_mqtt_transport
      RAISING   cx_apc_error.
    METHODS wait_and_process
      IMPORTING i_tcp           TYPE REF TO zif_mqtt_transport
                i_timeout       TYPE i
                i_handler       TYPE REF TO zif_amq_deamon
      RETURNING VALUE(r_result) TYPE string
      RAISING   zcx_mqtt.
    METHODS create_handler
      IMPORTING i_handler_class_name TYPE zamq_deamons-handler_class
      RETURNING VALUE(r_result)      TYPE REF TO zif_amq_deamon.
    METHODS write_appl_log
      IMPORTING i_text TYPE string
                i_type TYPE symsgty DEFAULT 'S'.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    DATA(deamon) = get_deamon( p_dguid ).
    DATA(broker) = get_broker( deamon-broker_name ).
    DATA(handler) = create_handler( deamon-handler_class ).

    DATA topic_strings TYPE string_table.
    SPLIT deamon-topics AT ',' INTO TABLE topic_strings.

    DATA(topics) = VALUE zcl_mqtt_packet_subscribe=>ty_topics( FOR topic IN topic_strings ( topic = topic ) ).

    write_appl_log( |Deamon { deamon-deamon_name } started at { sy-datlo DATE = USER } { sy-timlo TIME = USER }| ).

    TRY.
        DO.
          DATA(tcp) = create_tcp_transport( broker ).

          tcp->connect( ).

          TRY.
              tcp->send( NEW zcl_mqtt_packet_connect( iv_username = p_user iv_password = p_pass ) ).

              DATA(connack) = CAST zcl_mqtt_packet_connack( tcp->listen( 10 ) ).

              tcp->send( NEW zcl_mqtt_packet_subscribe(
                it_topics            = topics
                iv_packet_identifier = '0001' ) ).

              DATA(return_codes) = CAST zcl_mqtt_packet_suback( tcp->listen( 30 ) )->get_return_codes( ).

            CATCH zcx_mqtt_timeout.
              EXIT.
          ENDTRY.

          TRY.
              IF return_codes[ 1 ] = '00'.

                """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                " wait an hour for new messages
                " new message -> STOP message? -> disconnect and exit
                "             -> else process message in handler class and wait again
                " after one hour or timeout by broker -> reconnect
                """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
                DATA(message_string) = wait_and_process(
                  i_tcp = tcp
                  i_timeout = 3600
                  i_handler = handler
                ).

              ELSE.
                write_appl_log(
                  i_text = |Connection Error, Returncode' { return_codes[ 1 ] }|
                  i_type = 'E'
                ).
              ENDIF.
              """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

              tcp->send( NEW zcl_mqtt_packet_disconnect( ) ).
              tcp->disconnect( ).

              IF message_string = p_stop
              OR return_codes[ 1 ] <> '00'.
                EXIT.
              ENDIF.

            CATCH zcx_mqtt_timeout ##no_handler.
          ENDTRY.
        ENDDO.

      CATCH cx_apc_error
            zcx_mqtt INTO DATA(lcx_apc).
        WRITE: / lcx_apc->get_text( ).
        EXIT.
    ENDTRY.

    write_appl_log( |Deamon stopped at { sy-datlo DATE = USER } { sy-timlo TIME = USER }| ).

  ENDMETHOD.

  METHOD get_deamon.

    SELECT SINGLE * FROM zamq_deamons
      INTO @r_result
      WHERE guid = @p_dguid.

    IF sy-subrc <> 0.
      MESSAGE e952(00).       "not found
    ENDIF.

  ENDMETHOD.

  METHOD get_broker.

    SELECT SINGLE * FROM zamq_broker
      INTO @r_result
      WHERE broker_name = @i_brokername.

  ENDMETHOD.

  METHOD create_tcp_transport.

    r_result = zcl_mqtt_transport_tcp=>create(
      iv_host = i_broker-broker_host
      iv_port = CONV #( i_broker-broker_port )
      iv_protocol = SWITCH #( i_broker-use_ssl
                                WHEN abap_true
                                THEN cl_apc_tcp_client_manager=>co_protocol_type_tcps
                                ELSE cl_apc_tcp_client_manager=>co_protocol_type_tcp
                            )
    ).

  ENDMETHOD.

  METHOD wait_and_process.

    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    " wait an hour for new messages
    " new message -> STOP message? -> disconnect and exit
    "             -> else process message in handler class and wait again
    " after one hour or timeout by broker -> reconnect
    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

    DO.
      DATA(message) = CAST zcl_mqtt_packet_publish( i_tcp->listen( i_timeout ) )->get_message( ).
      r_result = cl_binary_convert=>xstring_utf8_to_string( message-message ).

      IF r_result = p_stop.
        EXIT.
      ENDIF.

      i_handler->on_receive( message ).
    ENDDO.

  ENDMETHOD.

  METHOD create_handler.

    TRY.
        CREATE OBJECT r_result TYPE (i_handler_class_name).
      CATCH cx_sy_create_object_error INTO DATA(lcx).
        MESSAGE lcx TYPE 'E'.
    ENDTRY.

  ENDMETHOD.

  METHOD write_appl_log.

    DATA log TYPE bal_s_log.
    DATA log_handle TYPE balloghndl.

    log-object = 'APPL_LOG'.
    log-subobject = 'OTHERS'.
    log-aldate_del = sy-datum + 7.

    CALL FUNCTION 'BAL_LOG_CREATE'
      EXPORTING
        i_s_log                 = log
      IMPORTING
        e_log_handle            = log_handle
      EXCEPTIONS
        log_header_inconsistent = 1
        OTHERS                  = 2.

    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    CALL FUNCTION 'BAL_LOG_MSG_ADD_FREE_TEXT'
      EXPORTING
        i_log_handle     = log_handle
        i_msgty          = i_type
        i_text           = CONV bapi_msg( i_text )
      EXCEPTIONS
        log_not_found    = 1
        msg_inconsistent = 2
        log_is_full      = 3
        OTHERS           = 4.

    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    CALL FUNCTION 'BAL_DB_SAVE'
      EXPORTING
        i_save_all = abap_true
      EXCEPTIONS
        OTHERS     = 0.

  ENDMETHOD.

ENDCLASS.
