import polars as pl
import time

def main():
    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/22_shizuoka_all.csv"
    # 出力ファイル
    out_file_path = "data/out_polars_csv.csv"

    # --- 処理 ---
    # 入力ファイルの読み込み
    try:
        df = pl.read_csv(
            source=in_file_path,
            has_header=True,
            separator=",",
            infer_schema_length=0, # 全ての列を str として読み込む pandas の dtype=str に相当
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

    filterd_df = df.filter(
        pl.col("name").str.contains("お茶", literal=True)
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"検索の時間 {elapsed_time}秒")

    print(f"フィルタリング後 {filterd_df.shape}")

    # 結果の保存
    filterd_df.write_csv(out_file_path)

if __name__ == "__main__":
    main()