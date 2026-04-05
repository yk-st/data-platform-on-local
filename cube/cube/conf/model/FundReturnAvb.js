/* eslint-disable cubejs/no-undef */
cube(`FundReturnABV`, {
  // ABV（vw_fund_daily_return）を直読み
  sql: `SELECT * FROM iceberg_prod.slv_fund.analytic_vw_fund_daily_return`,

  measures: {
    // 分子：半加算（時間SUMでOK）
    navDiffSum:         { sql: `nav_diff_per_10k`,           type: `sum`, title: `Σ NAV Diff (per 10k)`, shown: false},

    // 分母：粒度ごとに使い分け（いずれもSUM可能）
    navPrevSum:         { sql: `nav_prev_per_10k`,           type: `sum`, title: `Σ NAV Prev (per 10k)`, shown: false },        // 日
    navBegMonthEmitSum: { sql: `nav_beg_month_emit_per_10k`, type: `sum`, title: `Σ NAV Beg Month (emit)`, shown: false },     // 月
    navBegYearEmitSum:  { sql: `nav_beg_year_emit_per_10k`,  type: `sum`, title: `Σ NAV Beg Year (emit)`, shown: false },      // 年

    // 率：ratio-of-sums（粒度に合わせて選択）
    returnDailySimple: {
      sql: `CASE WHEN ${navPrevSum}=0 THEN NULL ELSE ${navDiffSum}/${navPrevSum} END`,
      type: `number`, title: `Return Daily (Simple)`, shown: false
    },
    returnMonthSimple: {
      sql: `CASE WHEN ${navBegMonthEmitSum}=0 THEN NULL ELSE ${navDiffSum}/${navBegMonthEmitSum} END`,
      type: `number`, title: `Return Month (Simple)`, shown: false
    },
    returnYearSimple: {
      sql: `CASE WHEN ${navBegYearEmitSum}=0 THEN NULL ELSE ${navDiffSum}/${navBegYearEmitSum} END`,
      type: `number`, title: `Return Year (Simple)`, shown: false
    },
    // 率（％値までCubeで作る：×100して返す）
    returnDailySimplePct: {
        sql: `100.0 * CASE WHEN ${navPrevSum}=0 THEN NULL ELSE ${navDiffSum}/${navPrevSum} END`,
        type: `number`,  // ← 単位は％。formatはnumberでOK（タイトルに%を明記）
        format: `percent`,
        title: `Return Daily (Simple, %)`
    },
    // 月・年も同様に％版を用意
    returnMonthSimplePct: {
        sql: `100.0 * CASE WHEN ${navBegMonthEmitSum}=0 THEN NULL ELSE ${navDiffSum}/${navBegMonthEmitSum} END`,
        type: `number`,
        format: `percent`,
        title: `Return Month (Simple, %)`
    },
    returnYearSimplePct: {
        sql: `100.0 * CASE WHEN ${navBegYearEmitSum}=0 THEN NULL ELSE ${navDiffSum}/${navBegYearEmitSum} END`,
        type: `number`,
        format: `percent`,
        title: `Return Year (Simple, %)`
    }
  },
  
  dimensions: {
    fundId:   { sql: `fund_id`,   type: `string` },
    fundType: { sql: `fund_type`, type: `string` },
    tradeTs:  { sql: `trade_ts`,  type: `time` }
  },

});
