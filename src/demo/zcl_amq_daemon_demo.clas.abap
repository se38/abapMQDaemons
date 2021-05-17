CLASS zcl_amq_daemon_demo DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    INTERFACES zif_amq_daemon.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_amq_daemon_demo IMPLEMENTATION.

  METHOD zif_amq_daemon~on_receive.

    DATA(appl_log) = NEW zcl_amq_appl_log( ).

    TRY.
        DATA(daemon) = zcl_amq_daemon=>get_daemon( i_daemon_guid ).
      CATCH zcx_amq_daemon INTO DATA(lcx).
        appl_log->add_message(
          i_message_type = 'E'
          i_text         = lcx->get_text( ) ).
        RETURN.
    ENDTRY.

    DATA(message_string) = cl_binary_convert=>xstring_utf8_to_string( i_message-message ).
    appl_log->add_message( |Daemon "{ daemon->get_daemon_name(  ) }" Topic "{ i_message-topic }" Message "{ message_string }"| ).

  ENDMETHOD.

ENDCLASS.
