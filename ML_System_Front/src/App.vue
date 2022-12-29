/* eslint-disable */
<template>
  <div style="width: auto; height: 400px" id="main"></div>
</template>

<script>

/* eslint-disable */
import * as echarts from "echarts"
import axios from 'axios'
      const axiosAPI= axios.create({
         baseURL:"http://localhost:8080",
       })
export default {
  name: "echarts",
  data() {
    return {}
  },
  mounted() {
    this.echartsInit()
  },
  methods: {
   async echartsInit() {
     await axiosAPI(`/ML/machineLearningResult/get`).then(res => { //根据学生Id查询成绩
          let rootData = res.data.data
          let boxDom = document.getElementById('main');
          let scoreCharts = echarts.init(boxDom);
          let option = {
            title: {
              text: 'Hadoop 可视化',
              left: 'center'
            },
            legend: {
              left: 'left'
            },
            xAxis: {
              type: 'value',
              name: '测试数据',
              data:[0, rootData.totalNumber],
              max: rootData.totalNumber
            },
            yAxis: {
              type: 'value',
              name: '预测成功数据',
              data:[0, rootData.predictTrueNumber],
              max: rootData.predictTrueNumber
            },
            series: [
              {
                name: '预测成功数',
                data: [
                  [0,0],
                  [rootData.totalNumber,rootData.predictTrueNumber]
                ],
                type: 'line',
              },
            ]
          };
          scoreCharts.setOption(option);


      })
      // let boxDom = document.getElementById('main');
      // let scoreCharts = echarts.init(boxDom);
      // let option = {
      //   title: {
      //     text: 'Hadoop 可视化',
      //     left: 'center'
      //   },
      //   legend: {
      //     left: 'left'
      //   },
      //   xAxis: {
      //     type: 'value',
      //     name: '所有数据',
      //   },
      //   yAxis: {
      //     type: 'value',
      //     name: '可信数据',
      //   },
      //   series: [
      //     {
      //       name: '100%可信',
      //       data: [
      //         [100, 100],
      //         [0, 0]
      //       ],
      //       type: 'line'
      //     },
      //     {
      //       name: '实际可信',
      //       data: [
      //         [0, 0],
      //         [100, 50]
      //       ],
      //       type: 'line'
      //     }
      //   ]
      // };
      // scoreCharts.setOption(option);
      // scoreCharts.on("mouseover", params => {
      //   console.log(params.value);
      // });
    }
  }
}
</script>
<style>
</style>
