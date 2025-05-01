# registry.py

from class_v.methods import (
    process_default,
    process_qa_list,
    process_qa_vip_list,
    process_qa_generic_list,
    process_qa_legendz_list,
    process_qa_turkish_list,
    process_qa_gsbj_list,
    process_permits,
    process_rotation,
    process_shuffle_rotation,
    process_turkish_rotation
)

PROCESSORS = {
    "process_default": process_default,
    "process_qa_list": process_qa_list,
    "process_qa_vip_list": process_qa_vip_list,
    "process_qa_generic_list": process_qa_generic_list,
    "process_qa_legendz_list": process_qa_legendz_list,
    "process_qa_turkish_list": process_qa_turkish_list,
    "process_qa_gsbj_list": process_qa_gsbj_list,
    "process_qa_tritonrl_list": process_qa_list,
    "process_permits": process_permits,
    "process_rotation": process_rotation,
    "process_shuffle_rotation": process_shuffle_rotation,
    "process_turkish_rotation": process_turkish_rotation
}


def proc_func(method: str, values: list) -> list:
    func = PROCESSORS.get(method)
    if not func:
        raise ValueError(f"❌ Метод '{method}' не зарегистрирован в PROCESSORS.")
    try:
        return func(values)
    except Exception as e:
        raise RuntimeError(f"⚠️ Ошибка при выполнении '{method}': {e}")
