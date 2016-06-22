<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<%@ page contentType="text/html; charset=iso-8859-1" language="java" %>
<%@ page import="java.net.*" %>
<%
    String sparql=request.getParameter("sparql");
    String infer=request.getParameter("infer");
    String auth=request.getParameter("auth");
    String vis=request.getParameter("vis");
	String resultFormat = request.getParameter("emit");
    String padding = request.getParameter("padding");

    if(sparql != null){
        String sparqlEnc = URLEncoder.encode(sparql,"UTF-8");
        String urlTo = "queryrdf?query.infer="+infer+"&query.auth="+auth+"&conf.cv="+vis+"&query.resultformat="+resultFormat+"&padding="+padding+"&query="+sparqlEnc;
        response.sendRedirect(urlTo);
    }
%>
<html>
<body>
<form name="sparqlQuery" method="post" action="sparqlQuery.jsp">
<table width="100%" border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td width="10%">&nbsp;</td>
    <td width="90%">&nbsp;</td>
    </tr>
  <tr>
    <td>SPARQL Query: </td>
    <td><textarea cols="150" rows="50" name="sparql">
Enter Sparql query here
    </textarea></td>
  </tr>
  <tr>
      <td>Inferencing?(true/false)</td>
      <td><INPUT TYPE=TEXT NAME="infer" SIZE="20"></td>
    </tr>
    <tr>
        <td>Authorization</td>
        <td><INPUT TYPE=TEXT NAME="auth" SIZE="20"></td>
    </tr>
    <tr>
        <td>visibility</td>
        <td><INPUT TYPE=TEXT NAME="vis" SIZE="20"></td>
    </tr>
		<tr>
			<td>Result Format</td>
			<td><select name="emit">
				<option value="xml">XML</option>
				<option value="json">JSON</option>
			</select></td>
		</tr>
		<tr>
			<td>JSONP Padding</td>
			<td><input type=text name="padding" size="20"></td>
		</tr>
  <tr>
    <td>&nbsp;</td>
    <td><input type="submit" name="submit" value="Submit"></td>
    </tr>
  <tr>
    <td>&nbsp;</td>
    <td>&nbsp;</td>
    </tr>
</table>
</form>
</body>
</html>
