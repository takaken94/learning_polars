import polars as pl

def load_dataframe(file_path: str, encoding: str, has_header: bool = False, separator: str = ",") -> pl.DataFrame:
    """指定されたパスからCSVファイルを読み込み、Polarsデータフレームを返す。"""
    try:
        return pl.read_csv(
            file_path,
            encoding=encoding,
            has_header=has_header,
            separator=separator,
        )
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません: {file_path}")
        exit(1)
    except Exception as e:
        print(f"エラー: ファイル読み込み中にエラーが発生しました: {e}")
        exit(1)

def main():
    """メイン処理"""
    # --- 設定 ---
    # データファイル
    data_file_path = "data/22_shizuoka_all_20210331.csv"
    data_encoding = "shift-jis"

    # 列名定義ファイル
    col_name_file_path = "data/mst_column_name.txt"
    col_name_file_encoding = "shift-jis"
    target_col_name_in_file = "column_name_en"

    # 出力ファイル
    out_csv_file_path = "data/22_shizuoka_all.csv"
    out_parq_file_path = "data/22_shizuoka_all.parquet"

    # --- 処理 ---
    # データの読み込み
    df = load_dataframe(data_file_path, data_encoding, has_header=False)

    # 列名マスタの読み込み
    col_name_df = load_dataframe(col_name_file_path, col_name_file_encoding, has_header=True, separator="\t")
    new_cols = col_name_df[target_col_name_in_file].to_list()
    # 列名の設定
    if len(df.columns) == len(new_cols):
        old_cols = df.columns
        df = df.rename(dict(zip(old_cols, new_cols)))
    else:
        print("エラー: データフレームの列数と新しい列名の数が一致しません。")
        print(f"データフレームの列数: {len(df.columns)}, 新しい列名の数: {len(new_cols)}")
        exit(1)

    # Polarsの表示設定
    # pl.Config.set_tbl_cols(-1)  # 表示する列数を無制限に設定

    # 統計量と先頭データの表示
    # print(df.describe())
    # print(df.head())

    # --- 処理 ---
    # データの保存
    try:
        df.write_csv(out_csv_file_path)
    except Exception as e:
        print(f"エラー: CSVファイルの保存中にエラーが発生しました: {e}")

    try:
        df.write_parquet(out_parq_file_path)
    except Exception as e:
        print(f"エラー: Parquetファイルの保存中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()