var notice = document.createElement('div')
var style = document.createElement('style')
style.type = 'text/css'
var css = '*{margin:0;padding:0;box-sizing:border-box}#app,body,html{background-color:#232429}.backward-alert{position:absolute;top:100px;width:100%;color:#fff;padding:20px;text-align:center}.backward-alert p{font-size:16px;margin-top:14px;font-weight:600}.backward-alert a{color:#00ab6b;display:inline-block;text-decoration:none;font-size:18px;margin-top:20px;padding:0 20px}.backward-alert a:active,a:hover{color:#42d885}'
if (style.styleSheet) {
  style.styleSheet.cssText = css
} else {
  style.appendChild(document.createTextNode(css))
}
notice.innerHTML = '<p>你当前使用的浏览器版本过低，无法正常使用本系统，建议使用更高版本浏览器。</p>\n' +
  '  <p>The browser version you are currently using is too low to use the system properly. It is recommended to use a higher version of the browser.</p>\n' +
  '  <a href="https://www.mozilla.org/" target="_blank">FireFox</a>\n' +
  '  <a href="https://www.google.com/intl/en/chrome/browser/" target="_blank">Chrome</a>'
notice.setAttribute('class', 'backward-alert')

document.body.appendChild(style)
document.body.appendChild(notice)
