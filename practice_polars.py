import polars as pl

def main():
    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/11_saitama_all.csv"

    # --- 処理 ---
    # 入力ファイルの読み込み
    try:
        df = pl.read_csv(
            source=in_file_path,
            infer_schema_length=0, # 全ての列を str として読み込む pandas の dtype=str に相当
        )
    except FileNotFoundError:
        print(f"入力ファイルが見つかりません: {in_file_path}")
        exit(1)
    except Exception as e:
        print(f"入力ファイル読み込み中にエラーが発生しました: {e}")
        exit(1)
    print(f"入力データの読み込み完了 {df.shape}")

    # 日付列の変換
    date_cols = [
        "updateDate",
        "changeDate",
        "closeDate",
        "assignmentDate",
    ]
    df = df.with_columns(
        pl.col(date_cols)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=True)
    )

    # 検索1
    result_df = df.filter(
         pl.col("corporateNumber") == "2000020111007"
    )
    print(f"フィルタリング後 {result_df.shape}")
    result_df.write_csv("data/out_polars1.csv")

    # 検索2
    corp_nums = [
        "1000020110001",
        "2000020111007",
        "9030005000249",
    ]
    result_df = df.filter(
        pl.col("corporateNumber").is_in(corp_nums)
    )
    print(f"フィルタリング後 {result_df.shape}")
    result_df.write_csv("data/out_polars2.csv")

    # 検索3
    result_df = df.filter(
        pl.col("assignmentDate") >= pl.date(2025, 12, 26)
    )
    print(f"フィルタリング後 {result_df.shape}")
    result_df.write_csv("data/out_polars3.csv")

    # 集計
    result = (
        df.group_by("cityName")
        .agg(
            pl.len().alias("count")
        )
        .sort("count", descending=True)
        .head(10)
    )
    pl.Config.set_tbl_rows(100)
    print(f"集計結果: {result}")

if __name__ == "__main__":
    main()