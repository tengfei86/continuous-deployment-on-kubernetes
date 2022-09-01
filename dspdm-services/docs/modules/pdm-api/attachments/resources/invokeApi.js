// ajax 对象
function ajaxObject() {
    var xmlHttp;
    try {
        // Firefox, Opera 8.0+, Safari
        xmlHttp = new XMLHttpRequest();
        } 
    catch (e) {
        // Internet Explorer
        try {
                xmlHttp = new ActiveXObject("Msxml2.XMLHTTP");
            } catch (e) {
            try {
                xmlHttp = new ActiveXObject("Microsoft.XMLHTTP");
            } catch (e) {
                alert("您的浏览器不支持AJAX！");
                return false;
            }
        }
    }
    return xmlHttp;
}
// ajax post请求：
function ajaxPost ( url , data , fnResponse, fnLoading ) {
    var ajax = ajaxObject();
    ajax.open( "post" , url , true );
    ajax.setRequestHeader( "Content-Type" , "application/json" );
    ajax.onreadystatechange = function () {
        if( ajax.readyState == 4 ) {
            fnResponse(ajax.status,ajax.responseText);
        } else {
            fnLoading();
        }
    }
    ajax.send( data );
}

function postCommon() {
    var url = "https://distplat3.landmarksoftware.io/services/dspdmservice/msp/secure/common";
    var data = document.getElementById("reqBody").value;
    document.getElementById("resBody").value = "start...";
    ajaxPost(url, data, function response(status, text) {
        document.getElementById("resBody").value = status + "\r\n" + text;
    }, function loading() {
        document.getElementById("resBody").value = "loading...";
    });
}