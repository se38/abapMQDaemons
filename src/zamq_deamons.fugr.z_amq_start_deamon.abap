FUNCTION z_amq_start_deamon.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(I_DGUID) TYPE  GUID_16
*"     VALUE(I_STOP) TYPE  CHAR20 DEFAULT 'STOP'
*"     VALUE(I_USER) TYPE  ZAMQ_USER
*"     VALUE(I_PASSWORD) TYPE  ZAMQ_PASSWORD
*"----------------------------------------------------------------------
  SUBMIT zamq_start_deamon
    WITH p_dguid = i_dguid
    WITH p_stop = i_stop
    WITH p_user = i_user
    WITH p_pass = i_password
  AND RETURN.

ENDFUNCTION.
