*&---------------------------------------------------------------------*
*& Report zamq_mqtt_send
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT zamq_mqtt_send.

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
*          iv_host = 'b-07186e60-6259-4cb1-9271-5de1d8cda2ff-1.mq.eu-west-1.amazonaws.com'
*          iv_port = '8883'
*          iv_protocol = cl_apc_tcp_client_manager=>co_protocol_type_tcps ).

        transport->connect( ).
*        transport->send( NEW zcl_mqtt_packet_connect( iv_username = 'se38' iv_password = 'Shu2tosh####' ) ).
        transport->send( NEW zcl_mqtt_packet_connect( ) ).

        DATA(connack) = CAST zcl_mqtt_packet_connack( transport->listen( 10 ) ).
        cl_demo_output=>write( |CONNACK return code: { connack->get_return_code( ) }| ).

        """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
        DO 3 TIMES.
          "send message to topic abap/test (dots will be converted to slashes)
          DATA(message) = VALUE zif_mqtt_packet=>ty_message(
            topic   = 'abap.test'
            message = cl_binary_convert=>string_to_xstring_utf8( 'Yet another message' ) ).

          transport->send( NEW zcl_mqtt_packet_publish( is_message = message ) ).
        ENDDO.

        message = VALUE zif_mqtt_packet=>ty_message(
          topic   = 'abap.test'
          message = cl_binary_convert=>string_to_xstring_utf8( 'STOP' ) ).

        transport->send( NEW zcl_mqtt_packet_publish( is_message = message ) ).

        """"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

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
