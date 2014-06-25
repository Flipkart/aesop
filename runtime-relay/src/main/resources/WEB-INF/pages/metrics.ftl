<#include "header.ftl">

	<div id="metrics">
	    <div class="section">
	        <h2>Inbound vs Outbound Stats</h2>
	        <div id="io-stats-container" class="metrics-container"></div>
	    </div>
	    <div class="section">
	        <h2>Relay Stats</h2>
	        <div id="relay-stats-container" class="metrics-container"></div>
	    </div>
	    <div class="section">
	        <h2>HTTP (Outbound) Stats</h2>
	        <div id="http-stats-container" class="metrics-container"></div>
	    </div>
	</div>

    <script type="text/javascript">
        $(document).ready(function() {
            Metrics.init();
        });
    </script>

<#include "footer.ftl">
