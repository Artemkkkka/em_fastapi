import pandas as pd
from scripts.hw import prepare_df


def test_prepare_df_with_valid_file(tmp_path):
    df = pd.DataFrame({
        "Код Инструмента": ["ABCD123X", "EFGH456Y"],
        "Наименование Инструмента": ["Oil A", "Oil B"],
        "Базис поставки": ["Basis1", "Basis2"],
        "Объем Договоров в единицах измерения": ["100", "200"],
        "Обьем Договоров": ["1000", "2000"],
        "Количество Договоров": ["1", "2"],
    })
    fake_file = tmp_path / "2025-07-20_test.xls"
    fake_file.write_text("placeholder")
    original_read_excel = pd.read_excel
    try:
        pd.read_excel = lambda *a, **k: {"Sheet1": df}
        result_df = prepare_df(str(fake_file))
    finally:
        pd.read_excel = original_read_excel

    assert not result_df.empty
    assert "exchange_product_id" in result_df.columns
    assert result_df.iloc[0]["oil_id"] == "ABCD"
    assert result_df.iloc[0]["count"] == 1
