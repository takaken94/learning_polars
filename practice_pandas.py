import pandas as pd

def main():
    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/11_saitama_all.csv"

    # --- 処理 ---
    # 入力ファイルの読み込み
    try:
        df = pd.read_csv(
            filepath_or_buffer=in_file_path,
            dtype=str,  # 全ての列を str として読み込む
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
    df[date_cols] = df[date_cols].apply(pd.to_datetime, format="%Y-%m-%d", errors="coerce")

    # 検索1
    result_df = df[df["corporateNumber"] == "2000020111007"]
    print(f"フィルタリング後 {result_df.shape}")
    result_df.to_csv("data/out_pandas1.csv", index=False)

    # 検索2
    corp_nums = [
        "1000020110001",
        "2000020111007",
        "9030005000249",
    ]
    result_df = df[df["corporateNumber"].isin(corp_nums)]
    print(f"フィルタリング後 {result_df.shape}")
    result_df.to_csv("data/out_pandas2.csv", index=False)

    # 検索3
    result_df = df[df["assignmentDate"] >= pd.Timestamp("2025-12-26")]
    print(f"フィルタリング後 {result_df.shape}")
    result_df.to_csv("data/out_pandas3.csv", index=False)

    # 集計
    result = (
        df.groupby("cityName")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(10)
    )
    pd.set_option("display.max_rows", 100)
    print(f"集計結果:\n{result}")

if __name__ == "__main__":
    main()