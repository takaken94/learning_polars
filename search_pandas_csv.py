import pandas as pd
import time

def main():
    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/22_shizuoka_all.csv"
    # 出力ファイル
    out_file_path = "data/out_pandas_csv.csv"

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

    # 検索
    start_time = time.time()

    filterd_df = df[
        df["name"].str.contains("お茶", regex=False, na=False)
    ]

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"検索の時間 {elapsed_time}秒")

    print(f"フィルタリング後 {filterd_df.shape}")

    # 結果の保存
    filterd_df.to_csv(out_file_path, index=False)

if __name__ == "__main__":
    main()