function serverAccordion() {
	var acc = document.getElementsByClassName("accordion");
	var i;

	for (i = 0; i < acc.length; i++) {
	  acc[i].addEventListener("click", function() {
	    this.classList.toggle("active");
	    var panel = this.nextElementSibling;
	    if (panel.style.maxHeight) {
	      panel.style.maxHeight = null;
	    } else {
	      panel.style.maxHeight = panel.scrollHeight + "px";
	    } 
	  });
	}
}

function isScheduled(select) {
	console.log(select)
	if(select) {
		selectOptionValue = document.getElementById("schedule-option").value;
		if(selectOptionValue == select.value) {
			document.getElementById("schedule-select").style.display = "block";
		}
		else {
			document.getElementById("schedule-select").style.display = "none";
		}
	}
	else {
		document.getElementById("schedule-select").style.display = "none";
	}
}