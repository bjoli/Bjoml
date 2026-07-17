// Copyright (C) 2026 Linus Björnstam <linus.internet@fastmail.se>
//
// This file is part of BjoML.
//
// BjoML is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Bjoml;

public class SimpleChannel<T>
{
    private readonly Queue<(T Value, Action ResumePut)> _puts = new();
    private readonly Queue<Action<T>> _gets = new();
    private readonly object _lock = new();

    public ValueTask PutMessage(T value)
    {
        Action<T>? getResume = null;
        
        lock (_lock)
        {
            if (_gets.Count > 0)
            {
                getResume = _gets.Dequeue();
            }
            else
            {
                var source = CmlValueTaskSource<Unit>.Rent();
                _puts.Enqueue((value, source.OnSyncVoidDelegate));
                return new ValueTask(source, source.Version);
            }
        }

        Scheduler.Dispatch(getResume, value);
        return default;
    }

    public ValueTask<T> GetMessage()
    {
        Action? putResume = null;
        T? value = default;

        lock (_lock)
        {
            if (_puts.Count > 0)
            {
                var put = _puts.Dequeue();
                value = put.Value;
                putResume = put.ResumePut;
            }
            else
            {
                var source = CmlValueTaskSource<T>.Rent();
                _gets.Enqueue(source.OnSyncDelegate);
                return new ValueTask<T>(source, source.Version);
            }
        }

        Scheduler.Dispatch(putResume);
        return new ValueTask<T>(value!);
    }
}
