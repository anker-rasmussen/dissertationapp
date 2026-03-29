(function retry() {
  var c = document.getElementById("lattice-canvas");
  if (!c) { setTimeout(retry, 50); return; }
  if (c.dataset.init) return;
  c.dataset.init = "1";
  var ctx = c.getContext("2d");
  var S = 80, N = 4, sp = 7.5;

  // 4x4x4 cubic lattice — clean grid, 64 nodes
  var base = [], edges = [];
  var idx = function (x, y, z) { return x * N * N + y * N + z; };
  var half = (N - 1) / 2;
  for (var x = 0; x < N; x++)
    for (var y = 0; y < N; y++)
      for (var z = 0; z < N; z++) {
        base.push([(x - half) * sp, (y - half) * sp, (z - half) * sp]);
        var i = idx(x, y, z);
        if (x < N - 1) edges.push([i, idx(x + 1, y, z)]);
        if (y < N - 1) edges.push([i, idx(x, y + 1, z)]);
        if (z < N - 1) edges.push([i, idx(x, y, z + 1)]);
      }

  var nTotal = base.length;
  var edgeAge = new Float32Array(edges.length);
  var dyingEdges = [];
  var nodeGlow = new Float32Array(nTotal);
  // Per-node rainfall timers — each node gets its own random countdown
  var rainTimer = new Float32Array(nTotal);
  for (var rt = 0; rt < nTotal; rt++) rainTimer[rt] = 60 + Math.floor(Math.random() * 300);

  var angle = 0;
  (function frame() {
    angle += 0.002;

    // Rainfall: each node independently pulses on its own random schedule
    for (var ri = 0; ri < nTotal; ri++) {
      rainTimer[ri]--;
      if (rainTimer[ri] <= 0) {
        rainTimer[ri] = 300 + Math.floor(Math.random() * 600); // 5-15s

        // 10% chance to rewire
        if (Math.random() > 0.1) continue;

        // Count edges on this node — must keep at least 1
        var count = 0;
        for (var ci = 0; ci < edges.length; ci++)
          if (edges[ci][0] === ri || edges[ci][1] === ri) count++;

        // Drop a random edge from this node (only if it has >1)
        if (count > 1) {
          for (var ei = edges.length - 1; ei >= 0; ei--) {
            if (edges[ei][0] === ri || edges[ei][1] === ri) {
              dyingEdges.push({ e: edges[ei], life: 20 });
              edges.splice(ei, 1);
              edgeAge = removeAt(edgeAge, ei);
              break;
            }
          }
        }

        // Add a new random edge from this node
        var to, tries = 0;
        do { to = Math.floor(Math.random() * nTotal); tries++; }
        while ((to === ri || dist3(base[ri], base[to]) > sp * 2) && tries < 40);
        if (to !== ri) {
          nodeGlow[ri] = 1.0;
          nodeGlow[to] = 0.6;
          edges.push([ri, to]);
          edgeAge = appendVal(edgeAge, 1.0);
        }
      }
    }

    // Decay
    for (var m = 0; m < nTotal; m++) if (nodeGlow[m] > 0.01) nodeGlow[m] *= 0.93; else nodeGlow[m] = 0;
    for (var ai = 0; ai < edgeAge.length; ai++) if (edgeAge[ai] > 0.01) edgeAge[ai] *= 0.9; else edgeAge[ai] = 0;
    for (var di2 = dyingEdges.length - 1; di2 >= 0; di2--) { dyingEdges[di2].life--; if (dyingEdges[di2].life <= 0) dyingEdges.splice(di2, 1); }

    // Rotate + project
    var cy = Math.cos(angle), sy = Math.sin(angle);
    var cx = Math.cos(0.5), sx = Math.sin(0.5);
    var proj = new Array(nTotal);
    for (var i = 0; i < nTotal; i++) {
      var bx = base[i][0], by = base[i][1], bz = base[i][2];
      var rx = bx * cy - bz * sy, rz = bx * sy + bz * cy;
      var ry = by * cx - rz * sx; rz = by * sx + rz * cx;
      var sc = 130 / (130 + rz);
      proj[i] = [S / 2 + rx * sc, S / 2 + ry * sc, rz, sc];
    }

    var col = getComputedStyle(c.parentElement).color;
    var pulse = c.parentElement.classList.contains("lattice-attaching");
    var ga = pulse ? 0.7 + 0.3 * Math.sin(Date.now() / 400) : 1;

    ctx.clearRect(0, 0, S, S);

    // Edges
    ctx.strokeStyle = col;
    for (var j = 0; j < edges.length; j++) {
      var p = proj[edges[j][0]], q = proj[edges[j][1]];
      var sa = (p[3] + q[3]) / 2;
      var fresh = edgeAge[j] > 0 ? (1 - edgeAge[j]) : 1;
      ctx.globalAlpha = sa * 0.4 * ga * fresh;
      ctx.lineWidth = sa * 0.9;
      ctx.beginPath(); ctx.moveTo(p[0], p[1]); ctx.lineTo(q[0], q[1]); ctx.stroke();
    }
    for (var dj = 0; dj < dyingEdges.length; dj++) {
      var de = dyingEdges[dj];
      var dp = proj[de.e[0]], dq = proj[de.e[1]];
      var dsa = (dp[3] + dq[3]) / 2;
      ctx.globalAlpha = dsa * 0.4 * ga * (de.life / 20);
      ctx.lineWidth = dsa * 0.9;
      ctx.beginPath(); ctx.moveTo(dp[0], dp[1]); ctx.lineTo(dq[0], dq[1]); ctx.stroke();
    }

    // Nodes — sorted back to front
    var order = [];
    for (var k = 0; k < nTotal; k++) order.push(k);
    order.sort(function (a, b) { return proj[b][2] - proj[a][2]; });

    ctx.fillStyle = col;
    for (var ki = 0; ki < order.length; ki++) {
      var k = order[ki];
      var pk = proj[k];
      var glow = nodeGlow[k];
      var r = pk[3] * (1.8 + glow * 2);
      ctx.globalAlpha = (0.3 + pk[3] * 0.6 + glow * 0.5) * ga;
      ctx.beginPath(); ctx.arc(pk[0], pk[1], r, 0, 6.283); ctx.fill();
    }

    requestAnimationFrame(frame);
  })();

  function dist3(a, b) {
    var dx = a[0]-b[0], dy = a[1]-b[1], dz = a[2]-b[2];
    return Math.sqrt(dx*dx+dy*dy+dz*dz);
  }
  function removeAt(arr, i) {
    var n = new Float32Array(arr.length - 1);
    for (var j = 0, k = 0; j < arr.length; j++) if (j !== i) n[k++] = arr[j];
    return n;
  }
  function appendVal(arr, v) {
    var n = new Float32Array(arr.length + 1);
    n.set(arr); n[arr.length] = v;
    return n;
  }
})();
