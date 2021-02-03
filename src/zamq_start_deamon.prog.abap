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
    DATA appl_log TYPE REF TO zcl_amq_appl_log.

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
                i_deamon        TYPE REF TO zcl_amq_deamon
      RETURNING VALUE(r_result) TYPE string
      RAISING   zcx_mqtt.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    appl_log = NEW #( ).

    TRY.
        DATA(deamon) = zcl_amq_deamon=>get_deamon( p_dguid ).
      CATCH zcx_amq_deamon INTO DATA(lcx_deamon).
        appl_log->add_message(
          i_message_type = 'E'
          i_text         = lcx_deamon->get_text( ) ).
        RETURN.
    ENDTRY.

    DATA(broker) = get_broker( deamon->get_broker_name(  ) ).
    DATA(topic_strings) = deamon->get_topics( ).
    DATA(topics) = VALUE zcl_mqtt_packet_subscribe=>ty_topics( FOR topic IN topic_strings ( topic = topic ) ).

    DATA(log_handle) = appl_log->add_message( |Deamon { deamon->get_deamon_name( ) } started at { sy-datlo DATE = USER } { sy-timlo TIME = USER }| ).
    deamon->set_log_handle( log_handle ).

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
                  i_deamon = deamon
                ).

              ELSE.
                appl_log->add_message(
                  i_text = |Connection Error, Returncode' { return_codes[ 1 ] }|
                  i_message_type = 'E'
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
        appl_log->add_message(
          i_message_type = 'E'
          i_text         = lcx_apc->get_text( ) ).
        EXIT.
    ENDTRY.

    appl_log->add_message( |Deamon stopped at { sy-datlo DATE = USER } { sy-timlo TIME = USER }| ).
    CLEAR log_handle.
    deamon->set_log_handle( log_handle ).

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

      i_deamon->handle_message( message ).
    ENDDO.

  ENDMETHOD.

ENDCLASS.
