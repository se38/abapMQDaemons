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
      RAISING
                cx_apc_error.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    DATA(deamon) = get_deamon( p_dguid ).
    DATA(broker) = get_broker( deamon-broker_name ).

    DATA handler TYPE REF TO zif_amq_deamon.

    TRY.
        CREATE OBJECT handler TYPE (deamon-handler_class).
      CATCH cx_sy_create_object_error INTO DATA(lcx).
        MESSAGE lcx TYPE 'E'.
    ENDTRY.

    DATA topic_strings TYPE string_table.
    SPLIT deamon-topics AT ',' INTO TABLE topic_strings.

    DATA(topics) = VALUE zcl_mqtt_packet_subscribe=>ty_topics( FOR topic IN topic_strings ( topic = topic ) ).

    TRY.
        DO.
          DATA(tcp) = create_tcp_transport( broker ).

          tcp->connect( ).

          TRY.
              tcp->send( NEW zcl_mqtt_packet_connect( iv_username = broker-broker_user iv_password = broker-broker_password ) ).

              DATA(connack) = CAST zcl_mqtt_packet_connack( tcp->listen( 10 ) ).

              tcp->send( NEW zcl_mqtt_packet_subscribe(
                it_topics            = topics
                iv_packet_identifier = '0001' ) ).

              DATA(return_codes) = CAST zcl_mqtt_packet_suback( tcp->listen( 30 ) )->get_return_codes( ).

            CATCH zcx_mqtt_timeout.
              EXIT.
          ENDTRY.

          IF return_codes[ 1 ] = '00'.

            """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
            " wait an hour for new messages
            " new message -> STOP message? -> disconnect and exit
            "             -> else process message in handler class and wait again
            " after one hour (= timeout) -> reconnect
            """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
            DO.
              TRY.
                  DATA(message) = CAST zcl_mqtt_packet_publish( tcp->listen( 3600 ) )->get_message( ).
                  DATA(message_string) = cl_binary_convert=>xstring_utf8_to_string( message-message ).

                  IF message_string = p_stop.
                    EXIT.
                  ENDIF.

                  handler->on_receive( message ).
                CATCH zcx_mqtt_timeout.
                  EXIT.
              ENDTRY.
            ENDDO.
          ELSE.
            WRITE:/ 'Connection Error, Returncode', return_codes[ 1 ].
          ENDIF.
          """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

          tcp->send( NEW zcl_mqtt_packet_disconnect( ) ).
          tcp->disconnect( ).

          IF message_string = p_stop
          OR return_codes[ 1 ] <> '00'.
            EXIT.
          ENDIF.
        ENDDO.

      CATCH cx_apc_error
            zcx_mqtt INTO DATA(lcx_apc).
        WRITE:/ lcx_apc->get_text( ).
        EXIT.
    ENDTRY.

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

ENDCLASS.
