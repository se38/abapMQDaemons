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

    DATA log TYPE bal_s_log.
    DATA log_handle TYPE balloghndl.

    log-object = 'APPL_LOG'.
    log-subobject = 'TEST'.
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

    DATA(message_string) = cl_binary_convert=>xstring_utf8_to_string( is_message-message ).

    CALL FUNCTION 'BAL_LOG_MSG_ADD_FREE_TEXT'
      EXPORTING
        i_log_handle     = log_handle
        i_msgty          = 'I'
        i_text           = CONV bapi_msg( |Topic: { is_message-topic } Message: { message_string }| )
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
        i_save_all       = abap_true
      EXCEPTIONS
        log_not_found    = 1
        save_not_allowed = 2
        numbering_error  = 3
        OTHERS           = 4.

    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

  ENDMETHOD.

ENDCLASS.
