/* eslint-disable cubejs/no-undef */
cube(`FundReturnAgg`, {
  sql: `SELECT * FROM iceberg_prod.gld_fund.agg_vw_fund_return_unified`,

  measures: {
    numSum:   { sql: `num_sum`,   type: `sum`, title: `Numerator (Σ NAV Diff)`, shown: false },
    denomSum: { sql: `denom_sum`, type: `sum`, title: `Denominator (Σ Basis)` , shown: false},

    // 粒度フリー：ratio-of-sums
    returnSimple: {
      sql: `CASE WHEN ${denomSum}=0 THEN NULL ELSE ${numSum}/${denomSum} END`,
      type: `number`,
      title: `Return (Simple)`,
      shown: false
    },

    // 粒度フリー：ratio-of-sums（%表示）
    returnSimplePct: {
      sql: `100 * CASE WHEN ${denomSum}=0 THEN NULL ELSE ${numSum}/${denomSum} END`,
      type: `number`,
      format: `percent`,
      title: `Return (Simple)`
    }
  },

  dimensions: {
    fundId:   { sql: `fund_id`,      type: `string` },
    fundType: { sql: `fund_type`,    type: `string` },
    period:   { sql: `period_start`, type: `time`   }, // TIMESTAMP 推奨
    grain:    { sql: `period_grain`, type: `string`, shown: false }
  },

  // ★ 最小追加：粒度セグメント
  segments: {
    isDay:   { sql: `${CUBE}.period_grain = 'day'` },
    isMonth: { sql: `${CUBE}.period_grain = 'month'` },
    isYear:  { sql: `${CUBE}.period_grain = 'year'` }
  }
});
