<#include "header.ftl">

    <div class="section">
	<h2>Registered Relays</h2>
	<table cellspacing=0 cellpadding=0 class="relays">
		<tr>
			<th>Physical Source</th>
			<th>Logical Source</th>
			<th>Producer</th>
			<th>Consumers</th>
		</tr>
		<#list relayInfos as relay>
			<tr>
				<td>
				    <table class="kv" cellspacing=0 cellpadding=0>
				        <tr><td class="k">ID</td><td class="v">${relay.getpSourceId()}</td></tr>
				        <tr><td class="k">Name</td><td class="v">${relay.getpSourceName()}</td></tr>
				        <tr><td class="k">URI</td><td class="v">${relay.getpSourceURI()}</td></tr>
				    </table>
				</td>
				<td>
				    <table class="kv" cellspacing=0 cellpadding=0>
				        <tr><td class="k">ID</td><td class="v">${relay.getlSourceId()}</td></tr>
				        <tr><td class="k">Name</td><td class="v">${relay.getlSourceName()}</td></tr>
				        <tr><td class="k">URI</td><td class="v">${relay.getlSourceURI()}</td></tr>
				    </table>
				</td>
				<td>
				    <table class="kv" cellspacing=0 cellpadding=0>
				        <tr><td class="k">Name</td><td class="v">${relay.getProducerName()}</td></tr>
				        <tr><td class="k">Consumers</td><td class="v">${relay.clientInfos?size}</td></tr>
				    </table>
				</td>
				<#if (relay.clientInfos?size > 0)>
				<td>
				    <table cellspacing=0 cellpadding=0 class="consumers">
                    <#list relay.clientInfos as clientInfo>
                        <tr>
                            <td style="border-right: 1px solid #b8c885">
                                <table class="kv" cellspacing=0 cellpadding=0>
                                    <tr><td class="k">Host</td><td class="v">${clientInfo.getClientName()}</td></tr>
                                    <tr><td class="k">Last SCN</td><td class="v">${clientInfo.getClientSinceSCN()}</td></tr>
                                </table>
                            </td>
                        </tr>
                    </#list>
                    </table>
                </td>
                <#else>
                    <td>None</td>
                </#if>
			</tr>		
		</#list>		
	</table>
	</div>


<#include "footer.ftl">
