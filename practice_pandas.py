import pandas as pd

def main():
    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/22_shizuoka_all.csv"
    # 出力ファイル
    out_file_path = "data/out_polars_csv.csv"

    # --- 処理 ---
    # 入力ファイルの読み込み
    try:
        df = pd.read_csv(
            filepath_or_buffer=in_file_path,
            dtype=str,  # 全ての列を str として読み込む
            sep=",",
        )
    except FileNotFoundError:
        print(f"入力ファイルが見つかりません: {in_file_path}")
        exit(1)
    except Exception as e:
        print(f"入力ファイル読み込み中にエラーが発生しました: {e}")
        exit(1)
    print(f"入力データの読み込み完了 {df.shape}")

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