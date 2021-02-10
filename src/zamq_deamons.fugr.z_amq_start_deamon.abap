FUNCTION z_amq_start_deamon.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(I_DGUID) TYPE  GUID_16
*"     VALUE(I_STOP) TYPE  STRING DEFAULT 'STOP'
*"     VALUE(I_USER) TYPE  ZAMQ_USER
*"     VALUE(I_PASSWORD) TYPE  ZAMQ_PASSWORD
*"----------------------------------------------------------------------
  TRY.
      zcl_amq_deamon=>get_deamon( i_dguid )->start(
        i_stop  = i_stop
        i_user  = i_user
        i_pass  = i_password
      ).
    CATCH zcx_amq_deamon ##no_handler.
  ENDTRY.

ENDFUNCTION.
