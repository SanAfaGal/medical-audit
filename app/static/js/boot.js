/*
 * Keeps the page hidden until it is really ready, so nobody sees it half built:
 *   - Alpine has started (otherwise every x-show element is visible for a moment),
 *   - the web fonts are in (otherwise the text shows in the default font and then jumps).
 *
 * The class is added from here, never written in the HTML: if this file fails to load the page simply
 * shows as before instead of staying blank. A timeout reveals it anyway after 3 s.
 * The matching rule (`html.preboot body { visibility: hidden }`) lives in base.html's <style>.
 */
(function () {
  'use strict';
  var root = document.documentElement;
  var revealed = false;

  function reveal() {
    if (revealed) return;
    revealed = true;
    root.classList.remove('preboot');
  }

  root.classList.add('preboot');

  var alpineReady = new Promise(function (resolve) {
    document.addEventListener('alpine:initialized', resolve, { once: true });
  });
  var fontsReady = document.fonts && document.fonts.ready ? document.fonts.ready : Promise.resolve();

  Promise.all([alpineReady, fontsReady]).then(function () {
    // two frames: let Alpine's first render and Tailwind's generated styles land before showing anything
    requestAnimationFrame(function () { requestAnimationFrame(reveal); });
  });
  setTimeout(reveal, 3000);
})();
