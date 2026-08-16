let context: AudioContext | null = null;

export function playRingtone(): () => void {
  if (typeof AudioContext === "undefined") return () => undefined;
  context = context ?? new AudioContext();
  const oscillator = context.createOscillator();
  const gain = context.createGain();
  oscillator.frequency.value = 660;
  gain.gain.value = 0.025;
  oscillator.connect(gain).connect(context.destination);
  oscillator.start();
  const timer = window.setTimeout(() => oscillator.stop(), 450);
  return () => {
    window.clearTimeout(timer);
    try {
      oscillator.stop();
    } catch {}
  };
}
