const { defineConfig } = require('@vue/cli-service')
module.exports = defineConfig({
  transpileDependencies: true
})
module.exports = {
  devServer: {
    port: 8080,

    host: "localhost",

    https: false,

    // 自动启动浏览器

    open: false,

    // assetsSubDirectory: 'static',
    // assetsPublicPath: '/api',
    proxy: {
      '/ML': {
        target: 'http://localhost:8080',
        changeOrigin: true,
        pathRewrite: {
          '^/ML': ''
        }
      }
    }
  }
}
