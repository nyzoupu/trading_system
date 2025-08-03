// 获取K线和CMACD相关数据，以及BOS High/Low数据
const [klineData, cmacdData, labelData, bosData] = await Promise.all([
  inputTable.getData([
    "open_time", "open", "close", "low", "high"
  ]),
  inputTable.getData([
    "open_time",
    "CMACD_macd",
    "CMACD_signal",
    "CMACD_histogram",
    "CMACD_macd_color",
    "CMACD_hist_color"
  ]),
  inputTable.getData([
    "open_time", "label"
  ]),
  inputTable.getData([
    "open_time", "SMC_is_BOS_High", "SMC_BOS_High_Value",
    "SMC_is_BOS_Low", "SMC_BOS_Low_Value"
  ])
]);

// K线数据处理
const klineCategory = klineData.map(row => row[0]);
const klineValues = klineData.map(row => [row[1], row[2], row[3], row[4]]); // open, close, low, high

// CMACD数据处理
const cmacdCategory = cmacdData.map(row => row[0]);
const cmacdMacd = cmacdData.map(row => row[1]);
const cmacdSignal = cmacdData.map(row => row[2]);
const cmacdHist = cmacdData.map(row => row[3]);
const cmacdMacdColor = cmacdData.map(row => row[4]);
const cmacdHistColor = cmacdData.map(row => row[5]);

// label数据处理
const labelMap = {};
labelData.forEach(([time, label]) => {
  labelMap[time] = label;
});

// 处理BOS High标注点
let bosHighPoints = [];
let inBosHigh = false;
let bosHighValue = null;
let bosHighStartIdx = null;

for (let i = 0; i < bosData.length; i++) {
  const isBosHigh = bosData[i][1] === "True";
  const bosHighVal = bosData[i][2];
  if (isBosHigh && !inBosHigh) {
    inBosHigh = true;
    bosHighValue = bosHighVal;
    bosHighStartIdx = i;
  }
  if (!isBosHigh && inBosHigh) {
    for (let j = bosHighStartIdx; j < i; j++) {
      bosHighPoints.push([
        bosData[j][0], // open_time
        bosHighValue
      ]);
    }
    inBosHigh = false;
    bosHighValue = null;
    bosHighStartIdx = null;
  }
}
if (inBosHigh) {
  for (let j = bosHighStartIdx; j < bosData.length; j++) {
    bosHighPoints.push([
      bosData[j][0], // open_time
      bosHighValue
    ]);
  }
}

// 处理BOS Low标注点
let bosLowPoints = [];
let inBosLow = false;
let bosLowValue = null;
let bosLowStartIdx = null;

for (let i = 0; i < bosData.length; i++) {
  const isBosLow = bosData[i][3] === "True";
  const bosLowVal = bosData[i][4];
  if (isBosLow && !inBosLow) {
    inBosLow = true;
    bosLowValue = bosLowVal;
    bosLowStartIdx = i;
  }
  if (!isBosLow && inBosLow) {
    for (let j = bosLowStartIdx; j < i; j++) {
      bosLowPoints.push([
        bosData[j][0], // open_time
        bosLowValue
      ]);
    }
    inBosLow = false;
    bosLowValue = null;
    bosLowStartIdx = null;
  }
}
if (inBosLow) {
  for (let j = bosLowStartIdx; j < bosData.length; j++) {
    bosLowPoints.push([
      bosData[j][0], // open_time
      bosLowValue
    ]);
  }
}

// 定义颜色映射
function getMacdColor(val) {
  if (val === 1) return '#ec0000'; // 红
  if (val === -1) return '#00da3c'; // 绿
  return '#888'; // 默认灰
}
function getHistColor(val) {
  if (val === 1) return '#FFD700'; // 金黄
  if (val === -1) return '#1E90FF'; // 蓝
  return '#888';
}

// 生成K线图的标注（label）series，放在K线上方
const klineLabelPoints = [];
klineData.forEach((row, idx) => {
  const open_time = row[0];
  const high = row[4];
  const label = labelMap[open_time];
  if (label !== undefined && label !== null && label !== "") {
    klineLabelPoints.push({
      value: [open_time, high * 1.01], // 最高价上方1%
      label: {
        show: true,
        formatter: label.toString(),
        color: '#222',
        fontWeight: 'normal',
        backgroundColor: 'transparent',
        borderRadius: 0,
        padding: 0,
        position: 'top'
      },
      symbol: 'circle',
      symbolSize: 1,
      itemStyle: {
        color: 'transparent'
      }
    });
  }
});

option = {
  title: {
    text: 'K线 + CMACD 指标 + BOS High/Low',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross'
    },
    // 修正：tooltip formatter 里显示 label 数据
    formatter: function(params) {
      // params 是数组，找到K线的 open_time
      let openTime = '';
      let klineIdx = -1;
      for (let i = 0; i < params.length; ++i) {
        if (params[i].seriesType === 'candlestick') {
          openTime = params[i].axisValue;
          klineIdx = params[i].dataIndex;
          break;
        }
      }
      if (!openTime && params.length > 0) {
        openTime = params[0].axisValue;
        klineIdx = params[0].dataIndex;
      }
      const label = labelMap[openTime];
      let html = `<div><strong>${openTime}</strong></div>`;
      for (let i = 0; i < params.length; ++i) {
        const p = params[i];
        if (p.seriesType === 'candlestick') {
          html += `<div>
            <span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>
            <b>K线</b><br/>
            开盘: ${p.data[0]}<br/>
            收盘: ${p.data[1]}<br/>
            最低: ${p.data[2]}<br/>
            最高: ${p.data[3]}<br/>
          </div>`;
        } else if (p.seriesName === 'CMACD_histogram') {
          html += `<div>
            <span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>
            CMACD_histogram: ${p.data.value !== undefined ? p.data.value : p.data}
          </div>`;
        } else if (p.seriesName === 'CMACD_macd') {
          html += `<div>
            <span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>
            CMACD_macd: ${p.data}
          </div>`;
        } else if (p.seriesName === 'CMACD_signal') {
          html += `<div>
            <span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>
            CMACD_signal: ${p.data}
          </div>`;
        } else if (p.seriesName === 'BOS High') {
          html += `<div>
            <span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>
            BOS High: ${p.data[1]}
          </div>`;
        } else if (p.seriesName === 'BOS Low') {
          html += `<div>
            <span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>
            BOS Low: ${p.data[1]}
          </div>`;
        }
      }
      if (label !== undefined && label !== null && label !== "") {
        html += `<div style="margin-top:4px;"><b>label:</b> ${label}</div>`;
      }
      return html;
    }
  },
  axisPointer: {
    link: [{ xAxisIndex: 'all' }]
  },
  legend: {
    data: ['K线', 'CMACD_macd', 'CMACD_signal', 'CMACD_histogram', 'BOS High', 'BOS Low'],
    top: 30
  },
  grid: [
    {
      left: '8%',
      right: '2%',
      top: 60,
      height: '45%' // 主图高度
    },
    {
      left: '8%',
      right: '2%',
      top: '60%',
      height: '25%' // 副图高度
    }
  ],
  xAxis: [
    {
      type: 'category',
      data: klineCategory,
      boundaryGap: true,
      axisLine: { onZero: false },
      splitLine: { show: false },
      min: 'dataMin',
      max: 'dataMax',
      gridIndex: 0,
      axisPointer: {
        show: true,
        type: 'line',
        label: {
          show: true
        }
      }
    },
    {
      type: 'category',
      data: klineCategory, // 统一时间轴
      boundaryGap: false,
      axisLine: { onZero: false },
      splitLine: { show: false },
      min: 'dataMin',
      max: 'dataMax',
      gridIndex: 1,
      axisPointer: {
        show: true,
        type: 'line',
        label: {
          show: true
        }
      }
    }
  ],
  yAxis: [
    {
      scale: true,
      splitArea: { show: true },
      gridIndex: 0
    },
    {
      scale: true,
      splitArea: { show: true },
      gridIndex: 1
    }
  ],
  dataZoom: [
    { type: 'inside', xAxisIndex: [0, 1], start: 80, end: 100 },
    { show: true, type: 'slider', xAxisIndex: [0, 1], top: '88%', start: 80, end: 100 }
  ],
  series: [
    // K线主图
    {
      name: 'K线',
      type: 'candlestick',
      data: klineValues,
      xAxisIndex: 0,
      yAxisIndex: 0,
      itemStyle: {
        color: '#ec0000',
        color0: '#00da3c',
        borderColor: '#ec0000',
        borderColor0: '#00da3c'
      }
    },
    // K线label标注，数字简洁显示
    {
      name: 'label',
      type: 'scatter',
      data: klineLabelPoints,
      xAxisIndex: 0,
      yAxisIndex: 0,
      symbol: 'circle',
      symbolSize: 1,
      label: {
        show: true,
        position: 'top'
      },
      itemStyle: {
        color: 'transparent'
      },
      z: 10
    },
    // BOS High标注点
    {
      name: 'BOS High',
      type: 'scatter',
      data: bosHighPoints,
      symbol: 'circle',
      symbolSize: 8,
      itemStyle: {
        color: '#e74c3c'
      },
      xAxisIndex: 0,
      yAxisIndex: 0,
      z: 5
    },
    // BOS Low标注点
    {
      name: 'BOS Low',
      type: 'scatter',
      data: bosLowPoints,
      symbol: 'circle',
      symbolSize: 8,
      itemStyle: {
        color: '#3498db'
      },
      xAxisIndex: 0,
      yAxisIndex: 0,
      z: 5
    },
    // CMACD副图
    {
      name: 'CMACD_histogram',
      type: 'bar',
      data: cmacdHist.map((v, i) => ({
        value: v,
        itemStyle: { color: getHistColor(cmacdHistColor[i]) }
      })),
      barWidth: 8,
      xAxisIndex: 1,
      yAxisIndex: 1,
      z: 1
    },
    {
      name: 'CMACD_macd',
      type: 'line',
      data: cmacdMacd,
      smooth: true,
      showSymbol: false,
      lineStyle: {
        width: 2,
        color: '#ec0000'
      },
      itemStyle: {
        color: function(params) {
          return getMacdColor(cmacdMacdColor[params.dataIndex]);
        }
      },
      xAxisIndex: 1,
      yAxisIndex: 1,
      z: 2
    },
    {
      name: 'CMACD_signal',
      type: 'line',
      data: cmacdSignal,
      smooth: true,
      showSymbol: false,
      lineStyle: {
        width: 2,
        color: '#1E90FF'
      },
      xAxisIndex: 1,
      yAxisIndex: 1,
      z: 2
    }
  ]
};



// 获取K线图和CMACD所需的数据
const rawData = await inputTable.getData([
  "open_time", "open", "close", "low", "high", "label",
  "K", "D", "J",
  "MA_5", "MA_10", "MA_20", "MA_42",
  "RSI6", "RSI12", "RSI24",
  "volume", "Volume_MA_5",
  "CMACD_macd", "CMACD_signal", "CMACD_histogram", "CMACD_macd_color", "CMACD_hist_color"
]);

// 处理K线主图数据
const categoryData = rawData.map(row => row[0]); // open_time
const values = rawData.map(row => [row[1], row[2], row[3], row[4]]);
const labels = rawData.map(row => row[5]);

// KDJ
const kData = rawData.map(row => row[6]);
const dData = rawData.map(row => row[7]);
const jData = rawData.map(row => row[8]);

// MA
const ma5Data = rawData.map(row => row[9]);
const ma10Data = rawData.map(row => row[10]);
const ma20Data = rawData.map(row => row[11]);
const ma42Data = rawData.map(row => row[12]);

// RSI
const rsi6Data = rawData.map(row => row[13]);
const rsi12Data = rawData.map(row => row[14]);
const rsi24Data = rawData.map(row => row[15]);

// VOL
const volData = rawData.map(row => row[16]);
const volMA5Data = rawData.map(row => row[17]);

// CMACD
const cmacd_macd = rawData.map(row => row[18]);
const cmacd_signal = rawData.map(row => row[19]);
const cmacd_histogram = rawData.map(row => row[20]);
const cmacd_macd_color = rawData.map(row => row[21]);
const cmacd_hist_color = rawData.map(row => row[22]);

// CMACD颜色映射
function getCMACDColor(val) {
  // 1: 红色, -1: 绿色, 0: 灰色
  if (val === 1) return '#ec0000';
  if (val === -1) return '#00da3c';
  return '#888888';
}

option = {
  title: {
    text: 'K线图',
    left: 'center'
  },
  tooltip: {
    trigger: 'axis',
    axisPointer: {
      type: 'cross'
    }
  },
  legend: {
    data: [
      'K线', 'MA5', 'MA10', 'MA20', 'MA42',
      'K', 'D', 'J',
      'RSI6', 'RSI12', 'RSI24',
      'VOL', 'VOL_MA5',
      'CMACD_histogram', 'CMACD_macd', 'CMACD_signal'
    ],
    top: 30
  },
  axisPointer: {
    link: [{ xAxisIndex: 'all' }]
  },
  // 使用 graphic 组件为每个子图添加标题
  graphic: [
    // 主图标题
    {
      type: 'text',
      left: '1%',
      top: '8%',
      style: {
        text: '主图',
        font: 'bold 14px sans-serif',
        fill: '#333'
      }
    },
    // CMACD标题
    {
      type: 'text',
      left: '1%',
      top: '52%',
      style: {
        text: 'CMACD',
        font: 'bold 14px sans-serif',
        fill: '#333'
      }
    },
    // VOL标题
    {
      type: 'text',
      left: '1%',
      top: '62%',
      style: {
        text: '成交量',
        font: 'bold 14px sans-serif',
        fill: '#333'
      }
    },
    // KDJ标题
    {
      type: 'text',
      left: '1%',
      top: '76%',
      style: {
        text: 'KDJ',
        font: 'bold 14px sans-serif',
        fill: '#333'
      }
    },
    // RSI标题
    {
      type: 'text',
      left: '1%',
      top: '86%',
      style: {
        text: 'RSI',
        font: 'bold 14px sans-serif',
        fill: '#333'
      }
    }
  ],
  grid: [
    { left: '8%', right: '2%', height: '38%' }, // K线主图
    { left: '8%', right: '2%', top: '52%', height: '8%' }, // CMACD
    { left: '8%', right: '2%', top: '62%', height: '10%' }, // VOL
    { left: '8%', right: '2%', top: '76%', height: '7%' },  // KDJ
    { left: '8%', right: '2%', top: '86%', height: '7%' }   // RSI
  ],
  xAxis: [
    {
      type: 'category',
      data: categoryData,
      scale: true,
      boundaryGap: false,
      axisLine: { onZero: false },
      splitLine: { show: false },
      min: 'dataMin',
      max: 'dataMax'
    },
    { type: 'category', gridIndex: 1, data: categoryData, show: false },
    { type: 'category', gridIndex: 2, data: categoryData, show: false },
    { type: 'category', gridIndex: 3, data: categoryData, show: false },
    { type: 'category', gridIndex: 4, data: categoryData, show: false }
  ],
  yAxis: [
    { scale: true, splitArea: { show: true } }, // K线主图
    { gridIndex: 1, splitNumber: 2, axisLabel: { show: false } }, // CMACD
    { gridIndex: 2, splitNumber: 2, axisLabel: { show: false } }, // VOL
    { gridIndex: 3, splitNumber: 2, axisLabel: { show: false } }, // KDJ
    { gridIndex: 4, splitNumber: 2, axisLabel: { show: false } }  // RSI
  ],
  dataZoom: [
    { type: 'inside', xAxisIndex: [0,1,2,3,4], start: 80, end: 100 },
    { show: true, xAxisIndex: [0,1,2,3,4], type: 'slider', top: '97%', start: 80, end: 100 }
  ],
  series: [
    // K线
    {
      name: 'K线',
      type: 'candlestick',
      data: values,
      itemStyle: {
        color: '#ec0000',
        color0: '#00da3c',
        borderColor: '#8A0000',
        borderColor0: '#008F28'
      }
    },
    // MA
    {
      name: 'MA5',
      type: 'line',
      data: ma5Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1 },
      color: '#FFD700'
    },
    {
      name: 'MA10',
      type: 'line',
      data: ma10Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1 },
      color: '#1E90FF'
    },
    {
      name: 'MA20',
      type: 'line',
      data: ma20Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1 },
      color: '#32CD32'
    },
    {
      name: 'MA42',
      type: 'line',
      data: ma42Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1 },
      color: '#8B008B'
    },
    // K线顶部label
    {
      name: 'Label',
      type: 'scatter',
      data: rawData.map(row => ({
        value: [row[0], row[4]],
        label: {
          show: true,
          position: 'top',
          formatter: row[5] != null ? String(row[5]) : ''
        }
      })),
      symbolSize: 1,
      tooltip: { show: false },
      z: 10,
      label: {
        show: true,
        position: 'top',
        color: '#333',
        fontSize: 12,
        formatter: function(params) {
          return params.data.label.formatter;
        }
      }
    },
    // CMACD histogram
    {
      name: 'CMACD_histogram',
      type: 'bar',
      xAxisIndex: 1,
      yAxisIndex: 1,
      data: cmacd_histogram,
      itemStyle: {
        color: function(params) {
          // 使用CMACD_hist_color列进行着色
          return getCMACDColor(cmacd_hist_color[params.dataIndex]);
        }
      }
    },
    // CMACD macd线
    {
      name: 'CMACD_macd',
      type: 'line',
      xAxisIndex: 1,
      yAxisIndex: 1,
      data: cmacd_macd,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, color: '#FFD700' }
    },
    // CMACD signal线
    {
      name: 'CMACD_signal',
      type: 'line',
      xAxisIndex: 1,
      yAxisIndex: 1,
      data: cmacd_signal,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, color: '#1E90FF' }
    },
    // VOL
    {
      name: 'VOL',
      type: 'bar',
      xAxisIndex: 2,
      yAxisIndex: 2,
      data: volData,
      itemStyle: { color: '#7fbe9e' }
    },
    {
      name: 'VOL_MA5',
      type: 'line',
      xAxisIndex: 2,
      yAxisIndex: 2,
      data: volMA5Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, color: '#FF8C00' }
    },
    // KDJ (单独一图)
    {
      name: 'K',
      type: 'line',
      xAxisIndex: 3,
      yAxisIndex: 3,
      data: kData,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, color: '#FF00FF' }
    },
    {
      name: 'D',
      type: 'line',
      xAxisIndex: 3,
      yAxisIndex: 3,
      data: dData,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, color: '#00BFFF' }
    },
    {
      name: 'J',
      type: 'line',
      xAxisIndex: 3,
      yAxisIndex: 3,
      data: jData,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, color: '#FFA500' }
    },
    // RSI (单独一图)
    {
      name: 'RSI6',
      type: 'line',
      xAxisIndex: 4,
      yAxisIndex: 4,
      data: rsi6Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, type: 'dashed', color: '#8B0000' }
    },
    {
      name: 'RSI12',
      type: 'line',
      xAxisIndex: 4,
      yAxisIndex: 4,
      data: rsi12Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, type: 'dashed', color: '#228B22' }
    },
    {
      name: 'RSI24',
      type: 'line',
      xAxisIndex: 4,
      yAxisIndex: 4,
      data: rsi24Data,
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 1, type: 'dashed', color: '#00008B' }
    }
  ]
};

// ========== 数据处理部分 ========== //
// 统一数据处理，所有变量只声明一次
const [klineData, cmacdData, labelData, bosData] = await Promise.all([
  inputTable.getData([
    "open_time", "open", "close", "low", "high"
  ]),
  inputTable.getData([
    "open_time",
    "CMACD_macd",
    "CMACD_signal",
    "CMACD_histogram",
    "CMACD_macd_color",
    "CMACD_hist_color"
  ]),
  inputTable.getData([
    "open_time", "label"
  ]),
  inputTable.getData([
    "open_time", "SMC_is_BOS_High", "SMC_BOS_High_Value",
    "SMC_is_BOS_Low", "SMC_BOS_Low_Value"
  ])
]);

const klineCategory = klineData.map(row => row[0]);
const klineValues = klineData.map(row => [row[1], row[2], row[3], row[4]]); // open, close, low, high

// label 数据
const labelMap = {};
labelData.forEach(([time, label]) => {
  labelMap[time] = label;
});

// BOS High/Low 标注点
let bosHighPoints = [], bosLowPoints = [];
let inBosHigh = false, bosHighValue = null, bosHighStartIdx = null;
let inBosLow = false, bosLowValue = null, bosLowStartIdx = null;
for (let i = 0; i < bosData.length; i++) {
  // BOS High
  const isBosHigh = bosData[i][1] === "True";
  const bosHighVal = bosData[i][2];
  if (isBosHigh && !inBosHigh) {
    inBosHigh = true; bosHighValue = bosHighVal; bosHighStartIdx = i;
  }
  if (!isBosHigh && inBosHigh) {
    for (let j = bosHighStartIdx; j < i; j++) {
      bosHighPoints.push([bosData[j][0], bosHighValue]);
    }
    inBosHigh = false; bosHighValue = null; bosHighStartIdx = null;
  }
  // BOS Low
  const isBosLow = bosData[i][3] === "True";
  const bosLowVal = bosData[i][4];
  if (isBosLow && !inBosLow) {
    inBosLow = true; bosLowValue = bosLowVal; bosLowStartIdx = i;
  }
  if (!isBosLow && inBosLow) {
    for (let j = bosLowStartIdx; j < i; j++) {
      bosLowPoints.push([bosData[j][0], bosLowValue]);
    }
    inBosLow = false; bosLowValue = null; bosLowStartIdx = null;
  }
}
if (inBosHigh) {
  for (let j = bosHighStartIdx; j < bosData.length; j++) {
    bosHighPoints.push([bosData[j][0], bosHighValue]);
  }
}
if (inBosLow) {
  for (let j = bosLowStartIdx; j < bosData.length; j++) {
    bosLowPoints.push([bosData[j][0], bosLowValue]);
  }
}

// K线顶部 label 标注点
const klineLabelPoints = [];
klineData.forEach((row, idx) => {
  const open_time = row[0];
  const high = row[4];
  const label = labelMap[open_time];
  if (label !== undefined && label !== null && label !== "") {
    klineLabelPoints.push({
      value: [open_time, high * 1.01],
      label: {
        show: true,
        formatter: label.toString(),
        color: '#222',
        fontWeight: 'normal',
        backgroundColor: 'transparent',
        borderRadius: 0,
        padding: 0,
        position: 'top'
      },
      symbol: 'circle',
      symbolSize: 1,
      itemStyle: { color: 'transparent' }
    });
  }
});

// 其余指标数据（用第二个 option 的处理方式）
const rawData = await inputTable.getData([
  "open_time", "open", "close", "low", "high", "label",
  "K", "D", "J",
  "MA_5", "MA_10", "MA_20", "MA_42",
  "RSI6", "RSI12", "RSI24",
  "volume", "Volume_MA_5",
  "CMACD_macd", "CMACD_signal", "CMACD_histogram", "CMACD_macd_color", "CMACD_hist_color"
]);
const ma5Data = rawData.map(row => row[9]);
const ma10Data = rawData.map(row => row[10]);
const ma20Data = rawData.map(row => row[11]);
const ma42Data = rawData.map(row => row[12]);
const kData = rawData.map(row => row[6]);
const dData = rawData.map(row => row[7]);
const jData = rawData.map(row => row[8]);
const rsi6Data = rawData.map(row => row[13]);
const rsi12Data = rawData.map(row => row[14]);
const rsi24Data = rawData.map(row => row[15]);
const volData = rawData.map(row => row[16]);
const volMA5Data = rawData.map(row => row[17]);
const cmacd_macd = rawData.map(row => row[18]);
const cmacd_signal = rawData.map(row => row[19]);
const cmacd_histogram = rawData.map(row => row[20]);
const cmacd_macd_color = rawData.map(row => row[21]);
const cmacd_hist_color = rawData.map(row => row[22]);

function getMacdColor(val) {
  if (val === 1) return '#ec0000';
  if (val === -1) return '#00da3c';
  return '#888';
}
function getHistColor(val) {
  if (val === 1) return '#FFD700';
  if (val === -1) return '#1E90FF';
  return '#888';
}
function getCMACDColor(val) {
  if (val === 1) return '#ec0000';
  if (val === -1) return '#00da3c';
  return '#888888';
}

// ========== option 合并 ========== //
option = {
  title: { text: 'K线+指标全览', left: 'center' },
  tooltip: {
    trigger: 'axis',
    axisPointer: { type: 'cross' },
    formatter: function(params) {
      let openTime = '';
      for (let i = 0; i < params.length; ++i) {
        if (params[i].seriesType === 'candlestick') {
          openTime = params[i].axisValue;
          break;
        }
      }
      if (!openTime && params.length > 0) {
        openTime = params[0].axisValue;
      }
      const label = labelMap[openTime];
      let html = `<div><strong>${openTime}</strong></div>`;
      for (let i = 0; i < params.length; ++i) {
        const p = params[i];
        if (p.seriesType === 'candlestick') {
          html += `<div><span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span><b>K线</b><br/>开盘: ${p.data[0]}<br/>收盘: ${p.data[1]}<br/>最低: ${p.data[2]}<br/>最高: ${p.data[3]}<br/></div>`;
        } else if (p.seriesName === 'CMACD_histogram') {
          html += `<div><span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>CMACD_histogram: ${p.data.value !== undefined ? p.data.value : p.data}</div>`;
        } else if (p.seriesName === 'CMACD_macd') {
          html += `<div><span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>CMACD_macd: ${p.data}</div>`;
        } else if (p.seriesName === 'CMACD_signal') {
          html += `<div><span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>CMACD_signal: ${p.data}</div>`;
        } else if (p.seriesName === 'BOS High') {
          html += `<div><span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>BOS High: ${p.data[1]}</div>`;
        } else if (p.seriesName === 'BOS Low') {
          html += `<div><span style="display:inline-block;margin-right:4px;border-radius:10px;width:10px;height:10px;background:${p.color};"></span>BOS Low: ${p.data[1]}</div>`;
        }
      }
      if (label !== undefined && label !== null && label !== "") {
        html += `<div style="margin-top:4px;"><b>label:</b> ${label}</div>`;
      }
      return html;
    }
  },
  legend: {
    data: [
      'K线', 'MA5', 'MA10', 'MA20', 'MA42',
      'K', 'D', 'J',
      'RSI6', 'RSI12', 'RSI24',
      'VOL', 'VOL_MA5',
      'CMACD_histogram', 'CMACD_macd', 'CMACD_signal',
      'BOS High', 'BOS Low', 'label'
    ],
    top: 30
  },
  axisPointer: { link: [{ xAxisIndex: 'all' }] },
  graphic: [
    { type: 'text', left: '1%', top: '8%', style: { text: '主图', font: 'bold 14px sans-serif', fill: '#333' } },
    { type: 'text', left: '1%', top: '52%', style: { text: 'CMACD', font: 'bold 14px sans-serif', fill: '#333' } },
    { type: 'text', left: '1%', top: '62%', style: { text: '成交量', font: 'bold 14px sans-serif', fill: '#333' } },
    { type: 'text', left: '1%', top: '76%', style: { text: 'KDJ', font: 'bold 14px sans-serif', fill: '#333' } },
    { type: 'text', left: '1%', top: '86%', style: { text: 'RSI', font: 'bold 14px sans-serif', fill: '#333' } }
  ],
  grid: [
    { left: '8%', right: '2%', height: '38%' }, // 主图
    { left: '8%', right: '2%', top: '52%', height: '8%' }, // CMACD
    { left: '8%', right: '2%', top: '62%', height: '10%' }, // VOL
    { left: '8%', right: '2%', top: '76%', height: '7%' },  // KDJ
    { left: '8%', right: '2%', top: '86%', height: '7%' }   // RSI
  ],
  xAxis: [
    { type: 'category', data: klineCategory, scale: true, boundaryGap: true, axisLine: { onZero: false }, splitLine: { show: false }, min: 'dataMin', max: 'dataMax' },
    { type: 'category', gridIndex: 1, data: klineCategory, show: false },
    { type: 'category', gridIndex: 2, data: klineCategory, show: false },
    { type: 'category', gridIndex: 3, data: klineCategory, show: false },
    { type: 'category', gridIndex: 4, data: klineCategory, show: false }
  ],
  yAxis: [
    { scale: true, splitArea: { show: true } }, // 主图
    { gridIndex: 1, splitNumber: 2, axisLabel: { show: false } }, // CMACD
    { gridIndex: 2, splitNumber: 2, axisLabel: { show: false } }, // VOL
    { gridIndex: 3, splitNumber: 2, axisLabel: { show: false } }, // KDJ
    { gridIndex: 4, splitNumber: 2, axisLabel: { show: false } }  // RSI
  ],
  dataZoom: [
    { type: 'inside', xAxisIndex: [0,1,2,3,4], start: 80, end: 100 },
    { show: true, xAxisIndex: [0,1,2,3,4], type: 'slider', top: '97%', start: 80, end: 100 }
  ],
  series: [
    // 主图 K线
    { name: 'K线', type: 'candlestick', data: klineValues, xAxisIndex: 0, yAxisIndex: 0, itemStyle: { color: '#ec0000', color0: '#00da3c', borderColor: '#ec0000', borderColor0: '#00da3c' } },
    // MA
    { name: 'MA5', type: 'line', data: ma5Data, smooth: true, showSymbol: false, lineStyle: { width: 1 }, color: '#FFD700', xAxisIndex: 0, yAxisIndex: 0 },
    { name: 'MA10', type: 'line', data: ma10Data, smooth: true, showSymbol: false, lineStyle: { width: 1 }, color: '#1E90FF', xAxisIndex: 0, yAxisIndex: 0 },
    { name: 'MA20', type: 'line', data: ma20Data, smooth: true, showSymbol: false, lineStyle: { width: 1 }, color: '#32CD32', xAxisIndex: 0, yAxisIndex: 0 },
    { name: 'MA42', type: 'line', data: ma42Data, smooth: true, showSymbol: false, lineStyle: { width: 1 }, color: '#8B008B', xAxisIndex: 0, yAxisIndex: 0 },
    // K线顶部 label
    { name: 'label', type: 'scatter', data: klineLabelPoints, xAxisIndex: 0, yAxisIndex: 0, symbol: 'circle', symbolSize: 1, label: { show: true, position: 'top' }, itemStyle: { color: 'transparent' }, z: 10 },
    // BOS High/Low
    { name: 'BOS High', type: 'scatter', data: bosHighPoints, symbol: 'circle', symbolSize: 8, itemStyle: { color: '#e74c3c' }, xAxisIndex: 0, yAxisIndex: 0, z: 5 },
    { name: 'BOS Low', type: 'scatter', data: bosLowPoints, symbol: 'circle', symbolSize: 8, itemStyle: { color: '#3498db' }, xAxisIndex: 0, yAxisIndex: 0, z: 5 },
    // CMACD
    { name: 'CMACD_histogram', type: 'bar', xAxisIndex: 1, yAxisIndex: 1, data: cmacd_histogram.map((v, i) => ({ value: v, itemStyle: { color: getHistColor(cmacd_hist_color[i]) } })), barWidth: 8, z: 1 },
    { name: 'CMACD_macd', type: 'line', xAxisIndex: 1, yAxisIndex: 1, data: cmacd_macd, smooth: true, showSymbol: false, lineStyle: { width: 2, color: '#ec0000' }, itemStyle: { color: function(params) { return getMacdColor(cmacd_macd_color[params.dataIndex]); } }, z: 2 },
    { name: 'CMACD_signal', type: 'line', xAxisIndex: 1, yAxisIndex: 1, data: cmacd_signal, smooth: true, showSymbol: false, lineStyle: { width: 2, color: '#1E90FF' }, z: 2 },
    // VOL
    { name: 'VOL', type: 'bar', xAxisIndex: 2, yAxisIndex: 2, data: volData, itemStyle: { color: '#7fbe9e' } },
    { name: 'VOL_MA5', type: 'line', xAxisIndex: 2, yAxisIndex: 2, data: volMA5Data, smooth: true, showSymbol: false, lineStyle: { width: 1, color: '#FF8C00' } },
    // KDJ
    { name: 'K', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: kData, smooth: true, showSymbol: false, lineStyle: { width: 1, color: '#FF00FF' } },
    { name: 'D', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: dData, smooth: true, showSymbol: false, lineStyle: { width: 1, color: '#00BFFF' } },
    { name: 'J', type: 'line', xAxisIndex: 3, yAxisIndex: 3, data: jData, smooth: true, showSymbol: false, lineStyle: { width: 1, color: '#FFA500' } },
    // RSI
    { name: 'RSI6', type: 'line', xAxisIndex: 4, yAxisIndex: 4, data: rsi6Data, smooth: true, showSymbol: false, lineStyle: { width: 1, type: 'dashed', color: '#8B0000' } },
    { name: 'RSI12', type: 'line', xAxisIndex: 4, yAxisIndex: 4, data: rsi12Data, smooth: true, showSymbol: false, lineStyle: { width: 1, type: 'dashed', color: '#228B22' } },
    { name: 'RSI24', type: 'line', xAxisIndex: 4, yAxisIndex: 4, data: rsi24Data, smooth: true, showSymbol: false, lineStyle: { width: 1, type: 'dashed', color: '#00008B' } }
  ]
};