FUNCTION Z_AMQ_START_DAEMON.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(I_DGUID) TYPE  GUID_16
*"     VALUE(I_STOP) TYPE  ZAMQ_STOP_MESSAGE DEFAULT 'STOP'
*"     VALUE(I_USER) TYPE  ZAMQ_USER
*"     VALUE(I_PASSWORD) TYPE  ZAMQ_PASSWORD
*"----------------------------------------------------------------------
  TRY.
      zcl_amq_daemon=>get_daemon( i_dguid )->start(
        i_stop  = i_stop
        i_user  = i_user
        i_pass  = i_password
      ).
    CATCH zcx_amq_daemon ##no_handler.
  ENDTRY.

ENDFUNCTION.
