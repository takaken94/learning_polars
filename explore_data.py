import polars as pl

def main():
    # --- 設定 ---
    # 入力ファイル
    in_file_path = "data/22_shizuoka_all.csv"

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
        print(f"ファイルが見つかりません: {in_file_path}")
        exit(1)
    except Exception as e:
        print(f"ファイル読み込み中にエラーが発生しました: {e}")
        exit(1)
    print(f"データの読み込み完了 {df.shape}")

    # ここにデータ探索のコードを追加してください


if __name__ == "__main__":
    main()