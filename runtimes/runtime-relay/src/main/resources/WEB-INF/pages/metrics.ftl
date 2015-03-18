<#include "header.ftl">

    <div class="section">
        <form>
        <table>
            <tr>
                <td>
                    Toggle Producers
                </td>
                <td>
                    <select name="producer">
                    <#list relayList as relay>
                        <option value="${ relay.pId }">${ relay.name } - ${ relay.producer }</option>
                    </#list>
                    </select>
                </td>
                <td>
                    <input type="submit" value="Redraw">
                </td>
            </tr>
        </table>

        </form>
    </div>

	<div id="metrics">
	</div>

    <script type="text/javascript">
        $(document).ready(function() {
            Dashboard.init(document.getElementById("metrics"));
        });
    </script>

<#include "footer.ftl">
