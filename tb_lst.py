from typing import Dict, Type

from runner_files.commodity import(
    CmdIntExp,
    CmdIntImp,
    CmdTrdDft
)

from runner_files.country import(
    CntryUniqueCd,
    CntryIntExp,
    CntryIntImp,
    CntryTrdDft
)


commodity_tb_lst: Dict[str, Type] = {
    "cmd_int_exp":CmdIntExp,
    "cmd_int_imp":CmdIntImp,
    "cmd_trd_dft":CmdTrdDft
}

country_tb_lst: Dict[str, Type] = {
    "cntry_unique_cd": CntryUniqueCd,
    "cntry_int_exp": CntryIntExp,
    "cntry_int_imp":  CntryIntImp,
    "cntry_trd_dft": CntryTrdDft
}

