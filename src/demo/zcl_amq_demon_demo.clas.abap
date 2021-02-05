CLASS zcl_amq_demon_demo DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES zif_amq_deamon.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_amq_demon_demo IMPLEMENTATION.

  METHOD zif_amq_deamon~on_receive.

    DATA(appl_log) = NEW zcl_amq_appl_log( ).

    TRY.
        DATA(deamon) = zcl_amq_deamon=>get_deamon( i_deamon_guid ).
      CATCH zcx_amq_deamon INTO DATA(lcx).
        appl_log->add_message(
          i_message_type = 'E'
          i_text         = lcx->get_text( ) ).
        RETURN.
    ENDTRY.

    DATA(message_string) = cl_binary_convert=>xstring_utf8_to_string( i_message-message ).
    appl_log->add_message( |Deamon { deamon->get_deamon_name(  ) } Topic { i_message-topic } Message { message_string }| ).

  ENDMETHOD.

ENDCLASS.
