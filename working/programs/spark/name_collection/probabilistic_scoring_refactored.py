"""
probabilistic_scoring_refactored.py – MinHashLSH (Spark ML) 構造化版
====================================================================
* fund_name と mgmt_company の **両方を比較** するよう改訂。
  正規化済み `fund_name_norm` + `company_norm` を連結 → Tokenizer → LSH。
* fee_diff を保持しつつ `lakehouse.temp.match_scores_ml` に保存。
* ML要素を関数分けして構造化。
"""
from spark_utils import get_spark
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, HashingTF, MinHashLSH
from typing import Tuple

# =============================================================================
# パラメータ定数
# =============================================================================
class MLParams:
    """機械学習パラメータ定数"""
    # 日本語トークナイザー用正規表現パターン
    TOKENIZER_PATTERN = r"[\p{IsHan}]+|[\p{IsHiragana}]+|[\p{IsKatakana}]+|[A-Za-z0-9]+"
    
    # ハッシュTF特徴量数
    TF_NUM_FEATURES = 512
    
    # MinHashLSHハッシュテーブル数
    LSH_NUM_HASH_TABLES = 8
    
    # 類似度閾値（1.0は全てのペアを取得）
    SIMILARITY_THRESHOLD = 1.0
    
    # テーブル名
    V2_TABLE = "lakehouse.temp.det_features_v2"
    LEGACY_TABLE = "lakehouse.temp.det_features_legacy"
    OUTPUT_TABLE = "lakehouse.temp.match_scores_ml"

# =============================================================================
# データ前処理関数
# =============================================================================
def load_and_preprocess_data(spark) -> Tuple:
    """
    データ読み込みと前処理
    
    Returns:
        Tuple: (v2_processed, legacy_processed)
    """
    # 1. 特徴量テーブル読み込み
    v2 = spark.table(MLParams.V2_TABLE)
    legacy = spark.table(MLParams.LEGACY_TABLE)
    
    # 2. 決定論ペアを除外
    pairs = (
        v2.select(F.col("fund_id").alias("v2_id"), "det_key")
              .join(
                  legacy.select(F.col("fund_id").alias("lg_id"), "det_key"),
                  on="det_key",
                  how="inner")
    )
    
    v2_un = v2.join(pairs, v2.fund_id == pairs.v2_id, "left_anti")
    legacy_un = legacy.join(pairs, legacy.fund_id == pairs.lg_id, "left_anti")
    
    # 3. テキスト特徴量作成（fund_name + company + nickname）
    def create_text_features(df):
        return (df
                .withColumn("nickname_norm", F.coalesce(F.col("nickname_norm"), F.lit("")))
                .withColumn("text", F.concat_ws(" ", "fund_name_norm", "company_norm", "nickname_norm")))
    
    v2_processed = create_text_features(v2_un)
    legacy_processed = create_text_features(legacy_un)
    
    return v2_processed, legacy_processed

# =============================================================================
# 特徴量変換関数
# =============================================================================
def create_feature_transformers():
    """
    特徴量変換パイプラインの作成
    
    Returns:
        Tuple: (tokenizer, hashing_tf)
    """
    # 日本語用トークナイザー
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="tokens",
        pattern=MLParams.TOKENIZER_PATTERN,
        gaps=False,
        toLowercase=False
    )
    
    # ハッシュTF
    hashing_tf = HashingTF(
        inputCol="tokens", 
        outputCol="tf", 
        numFeatures=MLParams.TF_NUM_FEATURES
    )
    
    return tokenizer, hashing_tf

def transform_features(df, tokenizer, hashing_tf):
    """
    データフレームに特徴量変換を適用
    
    Args:
        df: 入力データフレーム
        tokenizer: トークナイザー
        hashing_tf: ハッシュTF変換器
        
    Returns:
        変換済みデータフレーム
    """
    return hashing_tf.transform(tokenizer.transform(df))

# =============================================================================
# 学習関数
# =============================================================================
def train_lsh_model(v2_tf, legacy_tf):
    """
    MinHashLSHモデルの学習
    
    Args:
        v2_tf: v2特徴量データフレーム
        legacy_tf: legacy特徴量データフレーム
        
    Returns:
        学習済みLSHモデル
    """
    # 両方のデータを結合してモデル学習
    combined = v2_tf.select("tf").unionByName(legacy_tf.select("tf"))
    
    lsh_model = MinHashLSH(
        inputCol="tf", 
        outputCol="hashes", 
        numHashTables=MLParams.LSH_NUM_HASH_TABLES
    ).fit(combined)
    
    return lsh_model

# =============================================================================
# 予測関数
# =============================================================================
def predict_similarity(lsh_model, v2_tf, legacy_tf):
    """
    類似度予測とマッチング
    
    Args:
        lsh_model: 学習済みLSHモデル
        v2_tf: v2特徴量データフレーム
        legacy_tf: legacy特徴量データフレーム
        
    Returns:
        類似度マッチング結果
    """
    # LSH変換適用
    v2_vec = lsh_model.transform(v2_tf)
    legacy_vec = lsh_model.transform(legacy_tf)
    
    # 類似度計算（Jaccard距離）
    matches = (
        lsh_model.approxSimilarityJoin(
            v2_vec, legacy_vec, 
            MLParams.SIMILARITY_THRESHOLD, 
            distCol="jaccard_dist"
        )
        .select(
            F.col("datasetA.fund_id").alias("fund_id_v2"),
            F.col("datasetB.fund_id").alias("fund_id_legacy"),
            "jaccard_dist"
        )
    )
    
    return matches

def add_fee_difference(matches, v2_processed, legacy_processed):
    """
    手数料差分を追加
    
    Args:
        matches: マッチング結果
        v2_processed: v2処理済みデータ
        legacy_processed: legacy処理済みデータ
        
    Returns:
        手数料差分付きマッチング結果
    """
    return (
        matches
        .join(
            v2_processed.select(
                F.col("fund_id").alias("fid_v2"),
                F.col("trust_fee_rate").alias("trust_fee_rate_v2")
            ),
            matches.fund_id_v2 == F.col("fid_v2")
        )
        .join(
            legacy_processed.select(
                F.col("fund_id").alias("fid_lg"),
                F.col("trust_fee_rate").alias("trust_fee_rate_lg")
            ),
            matches.fund_id_legacy == F.col("fid_lg")
        )
        .withColumn("fee_diff", F.abs(F.col("trust_fee_rate_v2") - F.col("trust_fee_rate_lg")))
        .drop("trust_fee_rate_v2", "trust_fee_rate_lg", "fid_v2", "fid_lg")
    )

# =============================================================================
# メイン実行関数
# =============================================================================
def train_model(spark):
    """
    モデル学習のメイン関数
    
    Args:
        spark: SparkSession
        
    Returns:
        Tuple: (lsh_model, tokenizer, hashing_tf, v2_processed, legacy_processed)
    """
    print("🚀 モデル学習開始...")
    
    # 1. データ前処理
    print("📊 データ読み込みと前処理...")
    v2_processed, legacy_processed = load_and_preprocess_data(spark)
    
    # 2. 特徴量変換器作成
    print("🔧 特徴量変換器作成...")
    tokenizer, hashing_tf = create_feature_transformers()
    
    # 3. 特徴量変換
    print("🔄 特徴量変換実行...")
    v2_tf = transform_features(v2_processed, tokenizer, hashing_tf)
    legacy_tf = transform_features(legacy_processed, tokenizer, hashing_tf)
    
    # 4. モデル学習
    print("🎯 LSHモデル学習...")
    lsh_model = train_lsh_model(v2_tf, legacy_tf)
    
    print("✅ モデル学習完了!")
    return lsh_model, tokenizer, hashing_tf, v2_processed, legacy_processed

def predict_matches(spark, lsh_model, tokenizer, hashing_tf, v2_processed=None, legacy_processed=None):
    """
    マッチング予測のメイン関数
    
    Args:
        spark: SparkSession
        lsh_model: 学習済みLSHモデル
        tokenizer: トークナイザー
        hashing_tf: ハッシュTF変換器
        v2_processed: v2処理済みデータ（None の場合は再読み込み）
        legacy_processed: legacy処理済みデータ（None の場合は再読み込み）
        
    Returns:
        最終的なマッチング結果
    """
    print("🔍 マッチング予測開始...")
    
    # データが渡されていない場合は再読み込み
    if v2_processed is None or legacy_processed is None:
        print("📊 データ再読み込み...")
        v2_processed, legacy_processed = load_and_preprocess_data(spark)
    
    # 1. 特徴量変換
    print("🔄 特徴量変換...")
    v2_tf = transform_features(v2_processed, tokenizer, hashing_tf)
    legacy_tf = transform_features(legacy_processed, tokenizer, hashing_tf)
    
    # 2. 類似度予測
    print("🎯 類似度予測...")
    matches = predict_similarity(lsh_model, v2_tf, legacy_tf)
    
    # 3. 手数料差分追加
    print("💰 手数料差分計算...")
    final_matches = add_fee_difference(matches, v2_processed, legacy_processed)
    
    print("✅ マッチング予測完了!")
    return final_matches

def run_full_pipeline(spark, save_results=False):
    """
    完全なパイプライン実行
    
    Args:
        spark: SparkSession
        save_results: 結果をテーブルに保存するかどうか
        
    Returns:
        最終的なマッチング結果
    """
    print("🚀 確率的スコアリング完全パイプライン開始...")
    
    # 1. モデル学習
    lsh_model, tokenizer, hashing_tf, v2_processed, legacy_processed = train_model(spark)
    
    # 2. 予測実行
    final_matches = predict_matches(spark, lsh_model, tokenizer, hashing_tf, v2_processed, legacy_processed)
    
    # 3. 結果保存（オプション）
    # if save_results:
    #     print("💾 結果保存...")
    #     (final_matches.write.mode("overwrite")
    #             .format("iceberg")
    #             .saveAsTable(MLParams.OUTPUT_TABLE))
    #     print(f"✅ {MLParams.OUTPUT_TABLE} テーブルを保存しました")
    
    return final_matches

# =============================================================================
# 実行部分
# =============================================================================
if __name__ == "__main__":
    spark = get_spark()
    
    # 完全パイプライン実行
    matches = run_full_pipeline(spark, save_results=False)
    
    # 結果表示
    print("\n📊 マッチング結果サンプル:")
    matches.show(10, truncate=False)
    
    print(f"\n📈 総マッチング数: {matches.count()}")
    
    # 類似度分布確認
    print("\n🎯 Jaccard距離分布:")
    matches.select("jaccard_dist").describe().show()


# 🚀 確率的スコアリング完全パイプライン開始...
# 🚀 モデル学習開始...
# 📊 データ読み込みと前処理...
# 🔧 特徴量変換器作成...
# 🔄 特徴量変換実行...
# 🎯 LSHモデル学習...
# ✅ モデル学習完了!
# 🔍 マッチング予測開始...
# 🔄 特徴量変換...
# 🎯 類似度予測...
# 💰 手数料差分計算...
# ✅ マッチング予測完了!

# 📊 マッチング結果サンプル:
# +----------+--------------+------------------+--------+
# |fund_id_v2|fund_id_legacy|jaccard_dist      |fee_diff|
# +----------+--------------+------------------+--------+
# |FND002    |NFD002        |0.125             |0.00    |
# |FND003    |NFD003        |0.4               |0.00    |
# |FND003    |NFD002        |0.8461538461538461|0.10    |
# +----------+--------------+------------------+--------+


# 📈 総マッチング数: 3

# 🎯 Jaccard距離分布:
# +-------+-------------------+
# |summary|       jaccard_dist|
# +-------+-------------------+
# |  count|                  3|
# |   mean|0.45705128205128204|
# | stddev| 0.3639462241092665|
# |    min|              0.125|
# |    max| 0.8461538461538461|
# +-------+-------------------+



# | fund\_id\_v2 | fund\_id\_legacy | jaccard\_dist | 類似度 (= 1-dist) | fee\_diff | 評価コメント                                                             |
# | ------------ | ---------------- | ------------- | -------------- | --------- | ------------------------------------------------------------------ |
# | **FND002**   | **NFD002**       | **0.125**     | **0.875**      | 0.00      | ほぼ完全一致。<br>ニックネームの `(グッナイ)` と `(株)`→`B` の差だけで距離が少し残っただけ。**自動マージ可** |
# | **FND003**   | **NFD003**       | **0.40**      | **0.60**       | 0.00      | 名前・会社は一致。ニックネームの「(ゼンカン)」が効いて *1/3* ほどトークンがズレた状態。**要目視**だが十分類似      |
# | **FND003**   | **NFD002**       | **0.846**     | **0.154**      | 0.10      | 共有トークンは「ファンド／会社」程度で距離が大きい。**誤マッチ候補**（除外推奨）                         |