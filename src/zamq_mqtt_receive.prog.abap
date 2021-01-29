*&---------------------------------------------------------------------*
*& Report zamq_mqtt_receive
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT zamq_mqtt_receive.

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    TRY.
        DATA(transport) = zcl_mqtt_transport_tcp=>create(
          iv_host = '192.168.38.148'
          iv_port = '1883' ).

*        DATA(transport) = zcl_mqtt_transport_tcp=>create(
*          iv_host = 'b-d2d437a2-f577-48aa-a7c0-ed810de55a66-1.mq.eu-west-1.amazonaws.com'
*          iv_port = '8883'
*          iv_protocol = cl_apc_tcp_client_manager=>co_protocol_type_tcps ).

        transport->connect( ).
        transport->send( NEW zcl_mqtt_packet_connect( ) ).
*        transport->send( NEW zcl_mqtt_packet_connect( iv_username = 'se38' iv_password = 'Shu2tosh####' ) ).

        DATA(connack) = CAST zcl_mqtt_packet_connack( transport->listen( 10 ) ).
        cl_demo_output=>write( |CONNACK return code: { connack->get_return_code( ) }| ).

        """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
        "listening to topics abap/test and abap/newtest (dots will be converted into slashes)
        transport->send( NEW zcl_mqtt_packet_subscribe(
          it_topics            = VALUE #( ( topic = 'abap.test' ) ( topic = 'abap.newtest' ) )
          iv_packet_identifier = '0001' ) ).

        DATA(return_codes) = CAST zcl_mqtt_packet_suback( transport->listen( 30 ) )->get_return_codes( ).
        cl_demo_output=>write( |SUBACK return code: { return_codes[ 1 ] }| ).

        DO.
          DATA(message) = CAST zcl_mqtt_packet_publish( transport->listen( 30 ) )->get_message( ).
          data(message_string) = cl_binary_convert=>xstring_utf8_to_string( message-message ).
          cl_demo_output=>write( |{ message-topic } { message_string }| ).

          IF message_string = 'STOP'.
            EXIT.
          ENDIF.

        ENDDO.
        """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

        transport->send( NEW zcl_mqtt_packet_disconnect( ) ).
        transport->disconnect( ).

      CATCH zcx_mqtt_timeout.
        cl_demo_output=>write( 'timeout' ).

      CATCH cx_apc_error
            zcx_mqtt INTO DATA(lcx).
        cl_demo_output=>write( lcx->get_text( ) ).

    ENDTRY.

    cl_demo_output=>display( ).
  ENDMETHOD.

ENDCLASS.
