*&---------------------------------------------------------------------*
*& Report zamq_send_message_rest
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT zamq_send_message_rest.

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.
  PROTECTED SECTION.
  PRIVATE SECTION.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.

    cl_http_client=>create_by_url(
      EXPORTING
*        url                    = 'http://192.168.38.148:8161/api/message/testTopic.abap?type=topic'
        url                    = 'http://192.168.38.148:8161/api/message/testQueue.abap?type=queue'
      IMPORTING
        client                 = DATA(client)
      EXCEPTIONS
        argument_not_found     = 1
        plugin_not_active      = 2
        internal_error         = 3
        pse_not_found          = 4
        pse_not_distrib        = 5
        pse_errors             = 6
        OTHERS                 = 7
    ).
    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    client->authenticate(
      EXPORTING
        username             = 'admin'
        password             = 'admin'
    ).

    client->request->set_method( if_http_request=>co_request_method_post ).
    client->request->set_form_field( name = 'body' value = 'yet another test' ).

    client->send(
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3
        http_invalid_timeout       = 4
        OTHERS                     = 5
    ).
    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    client->receive(
      EXCEPTIONS
        http_communication_failure = 1
        http_invalid_state         = 2
        http_processing_failed     = 3
        OTHERS                     = 4 ).

    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    " Catch data
    DATA(stream) = client->response->get_cdata( ).

    client->close(
      EXCEPTIONS
        http_invalid_state = 1
        OTHERS             = 2
    ).
    IF sy-subrc <> 0.
      BREAK-POINT.
      RETURN.
    ENDIF.

    cl_demo_output=>display( stream ).

  ENDMETHOD.

ENDCLASS.
