(() => {
  const $ = (id) => document.getElementById(id);

  const policy = $("policy");
  const voiceBox = $("voiceBox");
  const enrollFace = $("enrollFace");
  const enrollVoice = $("enrollVoice");

  const btnStart = $("btnStart");
  const btnRecord = $("btnRecord");
  const btnStop = $("btnStop");
  const btnVerify = $("btnVerify");

  const challengeId = $("challengeId");
  const challengeText = $("challengeText");
  const timerEl = $("timer");

  const preview = $("preview");

  const finalDecision = $("finalDecision");
  const proofFile = $("proofFile");
  const breakdownEl = $("breakdown");
  const flagsEl = $("flags");

  let currentChallengeId = null;
  let countdownTimer = null;

  let mediaStream = null;
  let mediaRecorder = null;
  let recordedChunks = [];

  let recordedVideoBlob = null;
  let recordedAudioBlob = null; // for SPOKEN (may be inside video container too)
  let lastRecordedMime = null;

  function modeFromPolicy(p) {
    return p === "STRICT_STANDARD" ? "SPOKEN" : "SILENT";
  }

  function uiPolicyChanged() {
    const p = policy.value;
    voiceBox.style.display = (p === "STRICT_STANDARD") ? "block" : "none";
    // reset recording state
    btnVerify.disabled = true;
    recordedVideoBlob = null;
    recordedAudioBlob = null;
    preview.src = "";
  }

  policy.addEventListener("change", uiPolicyChanged);
  uiPolicyChanged();

  function setTimer(seconds) {
    timerEl.textContent = `${seconds}s`;
  }

  function startCountdown(seconds) {
    clearInterval(countdownTimer);
    let s = seconds;
    setTimer(s);
    countdownTimer = setInterval(() => {
      s -= 1;
      if (s <= 0) {
        clearInterval(countdownTimer);
        setTimer(0);
      } else {
        setTimer(s);
      }
    }, 1000);
  }

  async function startChallenge() {
    finalDecision.textContent = "—";
    proofFile.textContent = "";
    breakdownEl.textContent = "";
    flagsEl.textContent = "";

    const p = policy.value;
    const mode = modeFromPolicy(p);

    // Assumption: challenge engine exists at /api/challenge/start on same host or proxied
    // Here we call orchestrator host? We cannot know. We call directly CHALLENGE engine via same origin path "/api/challenge/start"
    // If you want cross-port, you must proxy or adjust.
    const body = {
      session_id: "UI-" + Math.random().toString(16).slice(2),
      mode: mode,
      locale: "it-IT"
    };

    const r = await fetch("/api/challenge/start", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(body)
    });

    const j = await r.json();
    if (!r.ok) throw new Error(JSON.stringify(j));

    currentChallengeId = j.challenge_id;
    challengeId.textContent = j.challenge_id || "—";
    challengeText.textContent = j.challenge_text || "—";
    const expiresAt = j.expires_at || null;

    // If expires_at is ISO, best effort parse; else use 30s default
    let seconds = 30;
    if (expiresAt) {
      const tExp = Date.parse(expiresAt);
      if (!Number.isNaN(tExp)) {
        const delta = Math.max(0, Math.floor((tExp - Date.now()) / 1000));
        seconds = Math.min(60, Math.max(5, delta));
      }
    }
    startCountdown(seconds);
  }

  btnStart.addEventListener("click", async () => {
    try {
      await startChallenge();
    } catch (e) {
      alert("Start Challenge failed:\n" + e.message);
    }
  });

  async function pickSupportedMime() {
    const candidates = [
      "video/webm;codecs=vp9,opus",
      "video/webm;codecs=vp8,opus",
      "video/webm",
      "video/mp4"
    ];
    for (const c of candidates) {
      if (MediaRecorder.isTypeSupported(c)) return c;
    }
    return "";
  }

  async function startRecording() {
    btnRecord.disabled = true;
    btnStop.disabled = false;
    btnVerify.disabled = true;
    recordedChunks = [];
    recordedVideoBlob = null;
    recordedAudioBlob = null;

    const p = policy.value;
    const mode = modeFromPolicy(p);

    const constraints = {
      video: true,
      audio: (mode === "SPOKEN")
    };

    mediaStream = await navigator.mediaDevices.getUserMedia(constraints);

    const mime = await pickSupportedMime();
    lastRecordedMime = mime || null;

    mediaRecorder = new MediaRecorder(mediaStream, mime ? { mimeType: mime } : undefined);
    mediaRecorder.ondataavailable = (ev) => {
      if (ev.data && ev.data.size > 0) recordedChunks.push(ev.data);
    };
    mediaRecorder.onstop = () => {
      const blob = new Blob(recordedChunks, { type: lastRecordedMime || "video/webm" });
      recordedVideoBlob = blob;

      const url = URL.createObjectURL(blob);
      preview.src = url;

      btnVerify.disabled = false;
      btnRecord.disabled = false;
      btnStop.disabled = true;

      // stop tracks
      mediaStream.getTracks().forEach(t => t.stop());
      mediaStream = null;
    };

    mediaRecorder.start(200); // collect chunks
  }

  btnRecord.addEventListener("click", async () => {
    try {
      if (!currentChallengeId) {
        alert("Prima fai Start Challenge.");
        return;
      }
      await startRecording();
    } catch (e) {
      btnRecord.disabled = false;
      btnStop.disabled = true;
      alert("Record failed:\n" + e.message);
    }
  });

  btnStop.addEventListener("click", async () => {
    try {
      btnStop.disabled = true;
      if (mediaRecorder && mediaRecorder.state !== "inactive") {
        mediaRecorder.stop();
      }
    } catch (e) {
      alert("Stop failed:\n" + e.message);
    }
  });

  async function verify() {
    if (!recordedVideoBlob) throw new Error("No recorded video");
    if (!currentChallengeId) throw new Error("No challenge_id");

    const p = policy.value;
    const mode = modeFromPolicy(p);

    // client-side validation
    if (!enrollFace.value.trim()) throw new Error("enrollment_id_face missing");
    if (p === "STRICT_STANDARD" && !enrollVoice.value.trim()) throw new Error("enrollment_id_voice missing");

    const fd = new FormData();
    fd.append("policy_id", p);
    fd.append("enrollment_id_face", enrollFace.value.trim());
    if (p === "STRICT_STANDARD") fd.append("enrollment_id_voice", enrollVoice.value.trim());
    fd.append("challenge_id", currentChallengeId);

    const vName = (lastRecordedMime && lastRecordedMime.includes("mp4")) ? "clip_video.mp4" : "clip_video.webm";
    fd.append("clip_video", recordedVideoBlob, vName);

    // IMPORTANT: In this UI we do not extract audio track separately; for SPOKEN we send same blob as clip_audio too
    // because many implementations accept audio-only OR container. Adjust later if needed.
    if (mode === "SPOKEN") {
      fd.append("clip_audio", recordedVideoBlob, "clip_audio.webm");
    }

    const r = await fetch("/api/multimodal/verify", { method: "POST", body: fd });
    const j = await r.json();

    if (!r.ok) {
      finalDecision.textContent = "ERROR";
      flagsEl.textContent = JSON.stringify(j, null, 2);
      return;
    }

    finalDecision.textContent = j.final_decision || "—";
    proofFile.textContent = j.proof_file ? `proof_file: ${j.proof_file}` : "";
    breakdownEl.textContent = "breakdown:\n" + JSON.stringify(j.breakdown || {}, null, 2);
    flagsEl.textContent = "flags:\n" + JSON.stringify(j.flags_summary || [], null, 2);
  }

  btnVerify.addEventListener("click", async () => {
    try {
      await verify();
    } catch (e) {
      alert("Verify failed:\n" + e.message);
    }
  });
})();
